#!/bin/bash
# Deploy BioProtect database objects.
#
# Usage:
#   bash db/deploy_all.sh              # Deploy everything
#   bash db/deploy_all.sh functions    # Deploy functions only
#   bash db/deploy_all.sh status       # Show migration status
#
# Environment variables (with defaults):
#   PGHOST=localhost PGPORT=5432 PGDATABASE=bioprotect
#   PGUSER=postgres  PGPASSWORD=postgres

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR/.."  # server/

COMMAND="${1:-deploy}"

case "$COMMAND" in
  functions|views|seeds|status|migrate)
    python db/migrate.py "$COMMAND"
    ;;
  deploy|"")
    python db/migrate.py deploy
    ;;
  dump-schema)
    # Generate a clean schema dump for reference
    PGPASSWORD="${PGPASSWORD:-postgres}" pg_dump \
      -h "${PGHOST:-localhost}" \
      -U "${PGUSER:-postgres}" \
      -d "${PGDATABASE:-bioprotect}" \
      --schema=bioprotect \
      --no-owner --no-privileges \
      -s \
      > db/schema_dump.sql
    echo "Schema dumped to db/schema_dump.sql"
    ;;
  *)
    echo "Usage: bash db/deploy_all.sh [deploy|functions|views|seeds|status|migrate|dump-schema]"
    exit 1
    ;;
esac
