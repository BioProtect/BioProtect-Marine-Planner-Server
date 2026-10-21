"""Self-check for the WDPA reference layer (migration 0013), on hand-made
polygons. No WDPA download needed.

    .venv/bin/python -W ignore test_wdpa.py

Applies the migration and the two functions inside a transaction on the
.env.local database and rolls back, so nothing is deployed or left behind.
"""

import os

import psycopg2
from dotenv import dotenv_values

HERE = os.path.dirname(os.path.abspath(__file__))

# Off Cork. A 2000-vertex circle (~30 km radius) forces ST_Subdivide; a
# designation overlapping it; a self-intersecting bowtie (invalid, must be
# repaired, not dropped); and a ~1 km² area to be culled at low zoom.
STAGING = """
CREATE TABLE bioprotect.wdpa_staging AS
SELECT * FROM (VALUES
  (1, 'Big Bay MPA', 'Marine Protected Area', 'IV', 'Designated', 2010,
   ST_Multi(ST_Buffer(ST_SetSRID(ST_MakePoint(-8.3, 51.6), 4326)::geography, 30000, 500)::geometry)),
  (2, 'Bay SAC', 'Special Area of Conservation', 'Not Reported', 'Designated', 2003,
   ST_Multi(ST_MakeEnvelope(-8.4, 51.5, -8.1, 51.7, 4326))),
  (3, 'Bowtie Reserve', 'Marine Reserve', 'II', 'Designated', 2018,
   ST_Multi(ST_GeomFromText('POLYGON((-9 51, -8.8 51.2, -8.8 51, -9 51.2, -9 51))', 4326))),
  (4, 'Tiny Cove', 'Marine Park', 'VI', 'Designated', 2021,
   ST_Multi(ST_MakeEnvelope(-8.0, 51.80, -7.99, 51.809, 4326)))
) v(wdpaid, name, desig_eng, iucn_cat, status, status_yr, geom);
"""


def main():
    c = dotenv_values(os.path.join(HERE, ".env.local"))
    conn = psycopg2.connect(dbname=c["db_name"], host=c["db_host"],
                            user=c["db_user"], password=c["db_pass"])
    cur = conn.cursor()
    try:
        for f in ("db/migrations/0013_wdpa.sql", "db/functions/wdpa_publish.sql",
                  "db/functions/wdpa_tiles.sql"):
            cur.execute(open(os.path.join(HERE, f)).read())
        cur.execute(STAGING)

        cur.execute("SELECT bioprotect.wdpa_publish('Oct2026')")
        assert cur.fetchone()[0] == 4, "every area, incl. the repaired bowtie"
        cur.execute("SELECT release FROM bioprotect.wdpa_release")
        assert cur.fetchall() == [("Oct2026",)]
        cur.execute("SELECT to_regclass('bioprotect.wdpa_staging'), to_regclass('bioprotect.wdpa_areas_new')")
        assert cur.fetchone() == (None, None), "staging and the build table are cleaned up"

        # the big circle really was subdivided
        cur.execute("SELECT count(*) FROM bioprotect.wdpa_areas WHERE wdpaid = 1 AND part = 'fill'")
        pieces = cur.fetchone()[0]
        assert pieces > 1, pieces

        # fill pieces rebuild the area; line pieces are its outline and nothing
        # more (no subdivision seams in the outline)
        cur.execute("""
            SELECT ST_Area(ST_Union(geom) FILTER (WHERE part = 'fill')::geography),
                   ST_Length(ST_Union(geom) FILTER (WHERE part = 'line')::geography),
                   ST_Perimeter(ST_Union(geom) FILTER (WHERE part = 'fill')::geography)
            FROM bioprotect.wdpa_areas WHERE wdpaid = 1""")
        area, outline, perimeter = cur.fetchone()
        assert abs(outline - perimeter) / perimeter < 1e-6, (outline, perimeter)
        assert abs(area / 1e6 - 3.1416 * 30 ** 2) / (3.1416 * 30 ** 2) < 0.01, area

        def tile(z, lon, lat):
            # the tile containing lon/lat at zoom z
            cur.execute("""
                SELECT bioprotect.wdpa_tiles(%(z)s,
                    floor((%(lon)s + 180) / 360 * 2 ^ %(z)s)::int,
                    floor((1 - ln(tan(radians(%(lat)s)) + 1 / cos(radians(%(lat)s))) / pi()) / 2 * 2 ^ %(z)s)::int)
            """, {"z": z, "lon": lon, "lat": lat})
            return bytes(cur.fetchone()[0])

        t10 = tile(10, -8.3, 51.6)
        for s in (b"wdpa_fill", b"wdpa_line", b"Big Bay MPA", b"Bay SAC",
                  b"Special Area of Conservation", b"wdpaid", b"iucn_cat"):
            assert s in t10, s
        assert b"Tiny Cove" in tile(12, -7.995, 51.805), "small areas show close up"
        t3 = tile(3, -8.3, 51.6)
        assert b"Big Bay MPA" in t3, "big areas show at low zoom"
        assert b"Tiny Cove" not in t3, "sub-pixel areas are culled at low zoom"
        assert tile(10, 150, -30) == b"", "an empty tile is empty"

        # a second load replaces the first (the swap path with existing data)
        cur.execute(STAGING)
        cur.execute("DELETE FROM bioprotect.wdpa_staging WHERE wdpaid <> 2")
        cur.execute("SELECT bioprotect.wdpa_publish('Nov2026')")
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT release FROM bioprotect.wdpa_release")
        assert cur.fetchall() == [("Nov2026",)]
        cur.execute("SELECT count(*) FROM pg_indexes WHERE indexname = 'wdpa_areas_geom_gist'")
        assert cur.fetchone()[0] == 1, "the index survives the swap"
    finally:
        conn.rollback()
        conn.close()
    print("ok")


if __name__ == "__main__":
    main()
