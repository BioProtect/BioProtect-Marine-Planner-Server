#!/usr/bin/env bash
# Export an empty-install copy of the bioprotect schema for a new deployment.
#
#   bash db/export_install.sh            (run from server/)
#
# Writes db/install/:
#   01_schema.sql  extensions + bioprotect schema, no data
#   02_seed.sql    reference data + schema_migrations (so migrate.py only
#                  runs migrations newer than this export)
#
# Load into an empty database, in order:
#   psql -d <db> -v ON_ERROR_STOP=1 -f 01_schema.sql -f 02_seed.sql
# or drop both into the Postgres container's /docker-entrypoint-initdb.d/.
# Functions are in the schema dump; no separate db/functions/ deploy needed.
#
# Not included, load separately: land (db/load_land.sh), WDPA (db/load_wdpa.sh,
# never ship WDPA data - licence forbids it).
#
# Regenerate after every new migration.
set -euo pipefail
cd "$(dirname "$0")/.."

env_val() { grep -E "^$1=" .env.local | head -1 | cut -d= -f2- | tr -d "\"'"; }
export PGHOST=$(env_val db_host) PGUSER=$(env_val db_user) \
       PGPASSWORD=$(env_val db_pass) PGDATABASE=$(env_val db_name)

# Dump with the server's own major version: a newer pg_dump writes settings
# (e.g. transaction_timeout) an older server rejects on load.
major=$(psql -Atc "SELECT current_setting('server_version_num')::int / 10000")
pg_dump() { "/usr/lib/postgresql/$major/bin/pg_dump" "$@"; }
[ -x "/usr/lib/postgresql/$major/bin/pg_dump" ] || unset -f pg_dump

OUT=db/install
mkdir -p "$OUT"

# Created per upload / per grid / per run by the app - never ship.
DYNAMIC=(f_* activity_* prioritizr_input_run_* tmp_rast_* v_h3_*)
# Legacy, unused by the code.
LEGACY=(res9_costs_staging gap_admin_start_project species_data
        wdpa_272_dissolved *_backup_km2migration gaul_eez_dissolved wdpa
        gaul_2015_simplified_1km ices_ecoregions)
SEED=(sensitivity_matrix pad jncc_sensitivities schema_migrations)

excludes=()
for t in "${DYNAMIC[@]}" "${LEGACY[@]}"; do excludes+=(-T "bioprotect.$t"); done

{
  echo "-- BioProtect schema, exported $(date -u +%F) from $PGDATABASE"
  echo "CREATE EXTENSION IF NOT EXISTS postgis;"
  echo "CREATE EXTENSION IF NOT EXISTS postgis_raster;"
  echo "CREATE EXTENSION IF NOT EXISTS h3;"
  pg_dump --schema-only --no-owner --no-privileges -n bioprotect "${excludes[@]}"
} > "$OUT/01_schema.sql"

seed=()
for t in "${SEED[@]}"; do seed+=(-t "bioprotect.$t"); done
pg_dump --data-only --no-owner --no-privileges "${seed[@]}" > "$OUT/02_seed.sql"

echo "Wrote $OUT/01_schema.sql ($(grep -c '^CREATE TABLE' "$OUT/01_schema.sql") tables)"
last=$(awk '/^COPY bioprotect.schema_migrations/{f=1;next} /^\\\./{f=0} f{print $1}' "$OUT/02_seed.sql" | sort | tail -1)
echo "Wrote $OUT/02_seed.sql (migrations up to $last)"
