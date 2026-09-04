#!/usr/bin/env python3
"""
BioProtect database migration runner.

Usage:
    python db/migrate.py              # Run all pending migrations
    python db/migrate.py status       # Show migration status
    python db/migrate.py new "desc"   # Create a new migration file
    python db/migrate.py deploy       # Deploy functions + views + migrations

Environment variables (with defaults):
    PGHOST=localhost  PGPORT=5432  PGDATABASE=bioprotect
    PGUSER=postgres   PGPASSWORD=postgres
"""

import os
import sys
import glob
import re
from datetime import datetime

import psycopg2

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DB_DIR = os.path.dirname(os.path.abspath(__file__))
MIGRATIONS_DIR = os.path.join(DB_DIR, "migrations")
FUNCTIONS_DIR = os.path.join(DB_DIR, "functions")
VIEWS_DIR = os.path.join(DB_DIR, "views")
SEEDS_DIR = os.path.join(DB_DIR, "seeds")

SCHEMA = "bioprotect"
TRACKING_TABLE = f"{SCHEMA}.schema_migrations"


def get_connection():
    return psycopg2.connect(
        host=os.getenv("PGHOST", "localhost"),
        port=int(os.getenv("PGPORT", "5432")),
        dbname=os.getenv("PGDATABASE", "bioprotect"),
        user=os.getenv("PGUSER", "postgres"),
        password=os.getenv("PGPASSWORD", "postgres"),
    )


def ensure_tracking_table(conn):
    """Create the migration tracking table if it doesn't exist."""
    with conn.cursor() as cur:
        cur.execute(f"""
            CREATE TABLE IF NOT EXISTS {TRACKING_TABLE} (
                version     TEXT PRIMARY KEY,
                name        TEXT NOT NULL,
                applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
            );
        """)
    conn.commit()


def get_applied_versions(conn):
    """Return set of already-applied migration versions."""
    with conn.cursor() as cur:
        cur.execute(f"SELECT version FROM {TRACKING_TABLE} ORDER BY version;")
        return {row[0] for row in cur.fetchall()}


def get_migration_files():
    """Return sorted list of (version, name, filepath) tuples."""
    pattern = os.path.join(MIGRATIONS_DIR, "*.sql")
    files = sorted(glob.glob(pattern))
    results = []
    for fp in files:
        basename = os.path.basename(fp)
        match = re.match(r"^(\d{4})_(.+)\.sql$", basename)
        if match:
            version = match.group(1)
            name = match.group(2)
            results.append((version, name, fp))
    return results


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------
def cmd_status(conn):
    """Show which migrations have been applied and which are pending."""
    applied = get_applied_versions(conn)
    migrations = get_migration_files()

    if not migrations:
        print("No migration files found.")
        return

    print(f"\n{'Version':<10} {'Status':<12} {'Name'}")
    print("-" * 60)
    for version, name, _ in migrations:
        status = "applied" if version in applied else "PENDING"
        print(f"{version:<10} {status:<12} {name}")

    pending = [m for m in migrations if m[0] not in applied]
    print(f"\n{len(applied)} applied, {len(pending)} pending.\n")


def cmd_migrate(conn):
    """Run all pending migrations in order."""
    applied = get_applied_versions(conn)
    migrations = get_migration_files()
    pending = [(v, n, fp) for v, n, fp in migrations if v not in applied]

    if not pending:
        print("Database is up to date. No pending migrations.")
        return

    for version, name, filepath in pending:
        print(f"  Applying {version}_{name}...", end=" ", flush=True)
        sql = open(filepath).read()

        try:
            with conn.cursor() as cur:
                # Run the migration
                cur.execute(sql)
                # Record it
                cur.execute(
                    f"INSERT INTO {TRACKING_TABLE} (version, name) VALUES (%s, %s);",
                    (version, name),
                )
            conn.commit()
            print("OK")
        except Exception as e:
            conn.rollback()
            print(f"FAILED\n\nError in migration {version}_{name}:\n{e}")
            sys.exit(1)

    print(f"\n{len(pending)} migration(s) applied successfully.")


def cmd_new(description):
    """Create a new migration file with the next sequence number."""
    os.makedirs(MIGRATIONS_DIR, exist_ok=True)
    migrations = get_migration_files()
    if migrations:
        last_version = int(migrations[-1][0])
    else:
        last_version = 0

    next_version = f"{last_version + 1:04d}"
    slug = re.sub(r"[^a-z0-9]+", "_", description.lower()).strip("_")
    filename = f"{next_version}_{slug}.sql"
    filepath = os.path.join(MIGRATIONS_DIR, filename)

    template = f"""-- Migration {next_version}: {description}
-- Created: {datetime.now().strftime('%Y-%m-%d %H:%M')}
--
-- This migration runs inside an implicit transaction.
-- Use IF NOT EXISTS / IF EXISTS for safety.
-- Never edit this file after it has been applied to any environment.

"""

    with open(filepath, "w") as f:
        f.write(template)

    print(f"Created: db/migrations/{filename}")
    return filepath


def cmd_deploy_objects(conn, obj_type):
    """Deploy all SQL files in a directory (functions, views, or seeds)."""
    dirs = {
        "functions": FUNCTIONS_DIR,
        "views": VIEWS_DIR,
        "seeds": SEEDS_DIR,
    }
    target_dir = dirs.get(obj_type)
    if not target_dir or not os.path.isdir(target_dir):
        print(f"No {obj_type}/ directory found. Skipping.")
        return 0

    files = sorted(glob.glob(os.path.join(target_dir, "*.sql")))
    if not files:
        print(f"No SQL files in {obj_type}/. Skipping.")
        return 0

    count = 0
    for fp in files:
        name = os.path.basename(fp)
        print(f"  Deploying {obj_type}/{name}...", end=" ", flush=True)
        sql = open(fp).read()
        try:
            with conn.cursor() as cur:
                cur.execute(sql)
            conn.commit()
            print("OK")
            count += 1
        except Exception as e:
            conn.rollback()
            print(f"FAILED\n    {e}")
            sys.exit(1)

    return count


def cmd_deploy_all(conn):
    """Run migrations first (tables/schema), then deploy functions, views, seeds."""
    print("=== Running migrations ===")
    cmd_migrate(conn)

    print("\n=== Deploying functions ===")
    f_count = cmd_deploy_objects(conn, "functions")

    print("\n=== Deploying views ===")
    v_count = cmd_deploy_objects(conn, "views")

    print("\n=== Deploying seeds ===")
    s_count = cmd_deploy_objects(conn, "seeds")

    print(f"\nDone. {f_count} functions, {v_count} views, {s_count} seeds deployed.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    args = sys.argv[1:]
    command = args[0] if args else "migrate"

    if command == "new":
        if len(args) < 2:
            print("Usage: python db/migrate.py new \"description\"")
            sys.exit(1)
        cmd_new(args[1])
        return

    conn = get_connection()
    ensure_tracking_table(conn)

    try:
        if command == "status":
            cmd_status(conn)
        elif command == "migrate":
            cmd_migrate(conn)
        elif command == "deploy":
            cmd_deploy_all(conn)
        elif command in ("functions", "views", "seeds"):
            cmd_deploy_objects(conn, command)
        else:
            print(f"Unknown command: {command}")
            print("Usage: python db/migrate.py [status|migrate|new|deploy|functions|views|seeds]")
            sys.exit(1)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
