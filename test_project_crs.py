"""Self-check for the per-project CRS (migration 0012).

    .venv/bin/python -W ignore test_project_crs.py

The DB half needs the database in .env.local. It applies the migration and
db/functions/area_km2.sql inside a transaction and rolls back, so nothing is
deployed.
"""

import os
import tempfile

import numpy as np
import psycopg2
import rasterio
from dotenv import dotenv_values
from rasterio.transform import from_origin

from services.raster_service import bounds_wgs84, clip_and_reproject

HERE = os.path.dirname(os.path.abspath(__file__))


def test_raster_without_crs_uses_fallback():
    """NZTM metres, no CRS on the file: read in the fallback, not as degrees."""
    with tempfile.TemporaryDirectory() as d:
        src, out = os.path.join(d, "src.tif"), os.path.join(d, "out.tif")
        # 10 km square near Wellington, in EPSG:2193 metres
        with rasterio.open(src, "w", driver="GTiff", height=10, width=10,
                           count=1, dtype="float32",
                           transform=from_origin(1748000, 5430000, 1000, 1000)) as dst:
            dst.write(np.ones((10, 10), dtype="float32"), 1)

        w, s, e, n = bounds_wgs84(src, "EPSG:2193")
        assert 174 < w < e < 176 and -42 < s < n < -40, (w, s, e, n)

        clip_and_reproject(src, out, fallback_crs="EPSG:2193")
        with rasterio.open(out) as r:
            assert r.crs.to_epsg() == 4326

        try:
            clip_and_reproject(src, out)
        except ValueError:
            pass
        else:
            raise AssertionError("a CRS-less raster with no fallback must fail")


def test_area_functions():
    c = dotenv_values(os.path.join(HERE, ".env.local"))
    conn = psycopg2.connect(dbname=c["db_name"], host=c["db_host"],
                            user=c["db_user"], password=c["db_pass"])
    try:
        cur = conn.cursor()
        for f in ("db/migrations/0012_project_crs.sql", "db/functions/area_km2.sql"):
            cur.execute(open(os.path.join(HERE, f)).read())

        # 0.1° square near Wellington
        sq = "ST_MakeEnvelope(174.7, -41.4, 174.8, -41.3, 4326)"
        cur.execute(f"""SELECT bioprotect.area_km2({sq}, NULL),
                               bioprotect.area_km2({sq}, 2193),
                               ST_Area({sq}::geography) / 1e6""")
        geog, nztm, expected = cur.fetchone()
        assert abs(geog - expected) < 1e-9, (geog, expected)
        assert abs(nztm - geog) / geog < 0.002, (nztm, geog)  # NZTM scale error

        # metres -> its own SRID; degrees and US feet -> NULL (geography)
        cur.execute("SELECT id FROM bioprotect.projects LIMIT 1")
        pid = cur.fetchone()[0]
        for epsg, expected in ((2193, 2193), (4326, None), (4167, None), (2263, None)):
            cur.execute("UPDATE bioprotect.projects SET crs_epsg = %s WHERE id = %s",
                        [epsg, pid])
            cur.execute("SELECT bioprotect.project_area_srid(%s)", [pid])
            got = cur.fetchone()[0]
            assert got == expected, (epsg, got)
    finally:
        conn.rollback()
        conn.close()


def test_antimeridian_guard():
    """A cell straddling 180° is rejected; a normal NZ cell is built."""
    import h3
    from handlers.planning_unit_websocket_handler import PlanningGridWSHandler as H
    from services.service_error import ServicesError

    ok = H.cells_to_gdf([h3.latlng_to_cell(-41.29, 174.78, 7)], 7, "x", "x")
    assert len(ok) == 1
    try:
        H.cells_to_gdf([h3.latlng_to_cell(-44.0, 179.999, 7)], 7, "x", "x")
    except ServicesError:
        pass
    else:
        raise AssertionError("a cell straddling 180° must be rejected")


if __name__ == "__main__":
    test_antimeridian_guard()
    test_raster_without_crs_uses_fallback()
    test_area_functions()
    print("ok")
