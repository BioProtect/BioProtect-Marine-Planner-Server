#!/bin/bash
# Load WDPA protected areas (marine + coastal polygons) into bioprotect.wdpa_areas
# (tables from migration 0013) for the "Reference layers" map layer.
#
# Download the WDPA File Geodatabase yourself from
#   https://www.protectedplanet.net/en/thematic-areas/wdpa?tab=WDPA
# (the download needs the terms of use accepted in a browser), then:
#
#   bash db/load_wdpa.sh /path/to/WDPA_Oct2026_Public.zip
#   bash db/load_wdpa.sh /path/to/WDPA_Oct2026_Public.gdb
#   bash db/load_wdpa.sh /path/to/download.zip --bbox -30,34,35,72   # clip: west,south,east,north
#
# Connection comes from the usual PG* variables, as in deploy_all.sh:
#   PGHOST=localhost PGPORT=5432 PGDATABASE=bioprotect PGUSER=postgres PGPASSWORD=postgres
#
# Replaces whatever is in bioprotect.wdpa_areas. The map keeps showing the old data
# until the new load is swapped in at the end. No Martin restart needed.
#
# Licence: WDPA may not be redistributed. Don't copy the data or this
# database's wdpa_areas table into anything you ship; each server loads its own.

set -euo pipefail

export PGHOST="${PGHOST:-localhost}" PGPORT="${PGPORT:-5432}" \
  PGDATABASE="${PGDATABASE:-bioprotect}" PGUSER="${PGUSER:-postgres}" \
  PGPASSWORD="${PGPASSWORD:-postgres}"

SRC="${1:-}"
[ -n "$SRC" ] || { sed -n '2,20p' "$0" >&2; exit 1; }
BBOX=""
if [ "${2:-}" = "--bbox" ]; then
  BBOX="${3:?--bbox needs west,south,east,north}"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

if [ "${SRC##*.}" = "zip" ]; then
  echo "Unzipping $SRC ..."
  unzip -q "$SRC" -d "$WORK"
  SRC="$(find "$WORK" -maxdepth 3 -type d -name '*.gdb' | head -n1)"
  [ -n "$SRC" ] || { echo "No .gdb inside the zip. Download the File Geodatabase version." >&2; exit 1; }
fi
[ -d "$SRC" ] || { echo "$SRC is not a .gdb folder or .zip" >&2; exit 1; }

# The polygon layer is named like WDPA_poly_Oct2026; the points layer is skipped.
# ogrinfo lists layers as "1: name (type)" or, in newer GDAL, "Layer: name (type)".
LAYER="$(ogrinfo -ro -q "$SRC" | sed -n -E 's/^([0-9]+:|Layer:) ([^ ]*poly[^ ]*).*/\2/p' | head -n1)"
[ -n "$LAYER" ] || { echo "No polygon layer in $SRC" >&2; ogrinfo -ro -q "$SRC" >&2; exit 1; }
# Release month for the map attribution, e.g. "Oct2026"
RELEASE="$(echo "$LAYER" | sed -n 's/.*_\([A-Z][a-z][a-z][0-9]\{4\}\)$/\1/p')"
RELEASE="${RELEASE:-$(basename "$SRC" .gdb)}"

SPAT=()
if [ -n "$BBOX" ]; then
  IFS=, read -r W S E N <<<"$BBOX"
  SPAT=(-spat "$W" "$S" "$E" "$N" -spat_srs EPSG:4326)
fi

echo "Loading $LAYER (marine and coastal only) into bioprotect.wdpa_staging ..."
ogr2ogr -f PostgreSQL PG:"" "$SRC" "$LAYER" -nln bioprotect.wdpa_staging -overwrite \
  -where "MARINE IN ('1','2')" \
  -select WDPAID,NAME,DESIG_ENG,IUCN_CAT,STATUS,STATUS_YR \
  -nlt PROMOTE_TO_MULTI -lco GEOMETRY_NAME=geom -lco SPATIAL_INDEX=NONE \
  -t_srs EPSG:4326 "${SPAT[@]}" -progress

echo "Subdividing and swapping in (release $RELEASE) ..."
psql -v ON_ERROR_STOP=1 -v release="$RELEASE" <<'SQL'
SELECT bioprotect.wdpa_publish(:'release') AS protected_areas;
SQL
