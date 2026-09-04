# BioProtect Database Management

## Structure

```
db/
  migrate.py          # Migration runner script
  migrations/         # Sequential, run-once schema changes
    0001_*.sql        # CREATE/ALTER TABLE, seed data, etc.
    0002_*.sql
  functions/          # CREATE OR REPLACE FUNCTION (idempotent, re-runnable)
  views/              # CREATE MATERIALIZED VIEW (idempotent, re-runnable)
  seeds/              # Reference data inserts (idempotent)
  deploy_all.sh       # Deploys all functions + views + runs pending migrations
```

## Quick Start

```bash
# Run pending migrations only
python db/migrate.py

# Deploy all functions + views + run migrations
bash db/deploy_all.sh

# Create a new migration
python db/migrate.py new "add_foo_table"
# → creates db/migrations/0003_add_foo_table.sql

# Check status (what's been applied, what's pending)
python db/migrate.py status

# Deploy functions only (safe to re-run anytime)
bash db/deploy_all.sh functions
```

## Rules

### Migrations (db/migrations/)
- **Never edit** a migration after it's been applied to any environment
- **Never delete** a migration
- Each migration runs inside a transaction (BEGIN/COMMIT)
- Use `IF NOT EXISTS` / `IF EXISTS` for safety
- For data migrations, always handle the "already migrated" case
- Name format: `NNNN_short_description.sql`

### Functions (db/functions/)
- One file per function, named after the function
- Always use `CREATE OR REPLACE FUNCTION`
- Safe to re-deploy at any time
- Changes to function signatures (args/return type) need a migration to DROP first

### Views (db/views/)
- One file per materialized view
- Use `CREATE MATERIALIZED VIEW IF NOT EXISTS` or handle DROP in a migration
- Refreshing views is separate from deploying them

### Seeds (db/seeds/)
- Use `INSERT ... ON CONFLICT DO NOTHING` or `DO UPDATE` for idempotency
- For reference data (sensitivity matrix, PAD pressures, etc.)
