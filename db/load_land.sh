#!/bin/bash
# Load OSM land polygons into bioprotect.land (table from migration 0011).
# Used to remove land from planning grids drawn on the map.
#
# Usage:
#   bash db/load_land.sh                 # download (~900 MB) and load
#   bash db/load_land.sh /path/to/land-polygons-split-4326[.zip]   # reuse a download
#
# Connection comes from the usual PG* variables, as in deploy_all.sh:
#   PGHOST=localhost PGPORT=5432 PGDATABASE=bioprotect PGUSER=postgres PGPASSWORD=postgres
#
# Takes a while (tens of minutes) and replaces whatever is in bioprotect.land.
# Data: © OpenStreetMap contributors, ODbL (https://www.openstreetmap.org/copyright).

set -euo pipefail

export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5432}" \
  PGDATABASE="${PGDATABASE:-bioprotect}" PGUSER="${PGUSER:-postgres}" \
  PGPASSWORD="${PGPASSWORD:-postgres}"

URL=https://osmdata.openstreetmap.de/download/land-polygons-split-4326.zip
SRC="${1:-}"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if [ -z "$SRC" ]; then
  echo "Downloading $URL ..."
  curl -fL --progress-bar -o "$WORK/land.zip" "$URL"
  SRC="$WORK/land.zip"
fi
if [ "${SRC##*.}" = "zip" ]; then
  unzip -q "$SRC" -d "$WORK"
  SRC="$WORK/land-polygons-split-4326"
fi
SHP="$SRC/land_polygons.shp"
[ -f "$SHP" ] || { echo "No land_polygons.shp in $SRC" >&2; exit 1; }

echo "Loading shapefile into bioprotect.land_raw ..."
ogr2ogr -f PostgreSQL PG:"" "$SHP" -nln bioprotect.land_raw -overwrite \
  -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geom -lco SPATIAL_INDEX=NONE \
  -a_srs EPSG:4326 -progress

echo "Subdividing into bioprotect.land ..."
# Small pieces keep the centroid-in-land check fast (17 s -> ~1 s per grid).
psql -v ON_ERROR_STOP=1 <<'SQL'
BEGIN;
TRUNCATE bioprotect.land RESTART IDENTITY;
INSERT INTO bioprotect.land (geom)
SELECT p.geom
FROM bioprotect.land_raw r,
     ST_Dump(r.geom) d,
     ST_Subdivide(d.geom, 256) s(geom),
     ST_Dump(s.geom) p
WHERE GeometryType(p.geom) = 'POLYGON';
DROP TABLE bioprotect.land_raw;
COMMIT;
ANALYZE bioprotect.land;
SELECT count(*) AS land_rows FROM bioprotect.land;
SQL
