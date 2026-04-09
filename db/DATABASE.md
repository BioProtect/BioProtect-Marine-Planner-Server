# BioProtect Database Documentation

## Table of Contents

- [1. Overview](#1-overview)
- [2. Fresh Installation](#2-fresh-installation)
- [3. Updating an Existing Database](#3-updating-an-existing-database)
- [4. Schema Reference](#4-schema-reference)
- [5. Functions Reference](#5-functions-reference)
- [6. Migration System](#6-migration-system)
- [7. Configuration](#7-configuration)
- [8. Backup and Restore](#8-backup-and-restore)
- [9. Troubleshooting](#9-troubleshooting)

---

## 1. Overview

BioProtect uses PostgreSQL with PostGIS and H3 extensions for marine conservation spatial planning.

| Component | Version |
|-----------|---------|
| PostgreSQL | 15.x |
| PostGIS | 3.4+ |
| PostGIS Raster | 3.5+ |
| h3-pg | 4.2+ |
| Schema | `bioprotect` |

### Architecture

```
Client (React) ──JSONP/POST──► Tornado (Python) ──aiopg──► PostgreSQL
                                    │                          │
                                    │ WebSocket + PTY           │
                                    ▼                          │
                              R (prioritizr) ──RPostgres───────┘
```

- **aiopg** pool: 2-10 async connections, 60s acquisition timeout
- **SQLAlchemy** engine: 3+10 connections, used for GeoDataFrame operations
- **RPostgres**: direct connection from R scripts, uses `PGHOST`/`PGUSER`/etc. env vars

---

## 2. Fresh Installation

### 2.1 Prerequisites

```bash
# PostgreSQL 15+
sudo apt install postgresql-15 postgresql-15-postgis-3

# h3-pg extension (https://github.com/zachasme/h3-pg)
# Follow h3-pg install instructions for your platform

# Python dependencies
pip install psycopg2-binary aiopg sqlalchemy geopandas
```

### 2.2 Create Database

```bash
sudo -u postgres psql <<'SQL'
CREATE DATABASE bioprotect;
\c bioprotect

-- Extensions
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS hstore;
CREATE EXTENSION IF NOT EXISTS h3;
CREATE EXTENSION IF NOT EXISTS postgres_fdw;

-- Schema
CREATE SCHEMA IF NOT EXISTS bioprotect;
SQL
```

### 2.3 Create Core Tables

```bash
sudo -u postgres psql -d bioprotect <<'SQL'

-- =============================================
-- Users and Authentication
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.users (
    id          SERIAL PRIMARY KEY,
    name        TEXT UNIQUE NOT NULL,
    email       TEXT,
    password    TEXT,
    role        TEXT DEFAULT 'User' CHECK (role IN ('Admin','User','ReadOnly')),
    created_at  TIMESTAMPTZ DEFAULT NOW()
);

-- =============================================
-- Projects
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.projects (
    id                      SERIAL PRIMARY KEY,
    name                    TEXT NOT NULL,
    description             TEXT,
    date_created            TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
    planning_unit_id        INTEGER,
    old_version             BOOLEAN,
    iucn_category           TEXT,
    is_private              BOOLEAN,
    costs                   TEXT,
    default_resolution      INTEGER DEFAULT 7,
    active_cost_profile_id  INTEGER  -- FK added after cost_profiles created
);

CREATE TABLE IF NOT EXISTS bioprotect.user_projects (
    id          SERIAL PRIMARY KEY,
    user_id     INTEGER NOT NULL REFERENCES bioprotect.users(id),
    project_id  INTEGER NOT NULL REFERENCES bioprotect.projects(id) ON DELETE CASCADE,
    role        TEXT DEFAULT 'owner'
);

-- =============================================
-- Planning Units (H3 hexagonal grid)
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.metadata_planning_units (
    unique_id               SERIAL PRIMARY KEY,
    feature_class_name      TEXT UNIQUE,
    alias                   TEXT,
    description             TEXT,
    domain                  TEXT DEFAULT 'marine',
    _area                   DOUBLE PRECISION,
    envelope                GEOMETRY,
    creation_date           TIMESTAMPTZ,
    source                  TEXT,
    created_by              TEXT,
    tilesetid               TEXT,
    planning_unit_count     INTEGER,
    extent                  BOX2D,
    resolution              INTEGER
);

CREATE TABLE IF NOT EXISTS bioprotect.h3_cells (
    h3_index        TEXT NOT NULL,
    resolution      INTEGER,
    scale_level     TEXT,
    project_area    TEXT,
    geometry        GEOMETRY(Polygon, 4326),
    area_km2        DOUBLE PRECISION
);
CREATE INDEX IF NOT EXISTS idx_h3_cells_index ON bioprotect.h3_cells (h3_index);
CREATE INDEX IF NOT EXISTS idx_h3_cells_geom ON bioprotect.h3_cells USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_h3_cells_area ON bioprotect.h3_cells (project_area, resolution);

CREATE TABLE IF NOT EXISTS bioprotect.project_pus (
    id          SERIAL PRIMARY KEY,
    project_id  INTEGER REFERENCES bioprotect.projects(id) ON DELETE CASCADE,
    h3_index    TEXT,
    UNIQUE (project_id, h3_index)
);
CREATE INDEX IF NOT EXISTS idx_project_pus_project_h3 ON bioprotect.project_pus (project_id, h3_index);

-- =============================================
-- Features (conservation interest features)
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.metadata_interest_features (
    unique_id               SERIAL PRIMARY KEY,
    feature_class_name      TEXT UNIQUE,
    alias                   TEXT,
    description             TEXT,
    creation_date           TIMESTAMPTZ,
    _area                   DOUBLE PRECISION,
    tilesetid               TEXT,
    extent                  BOX2D,
    source                  TEXT,
    created_by              TEXT
);

CREATE TABLE IF NOT EXISTS bioprotect.project_features (
    project_id          BIGINT NOT NULL,
    feature_unique_id   BIGINT NOT NULL,
    target_type         TEXT DEFAULT 'prop' CHECK (target_type IN ('prop','abs')),
    target_value        NUMERIC,            -- 0-100 percentage
    spf                 NUMERIC DEFAULT 40, -- Species Penalty Factor
    weight              NUMERIC,
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (project_id, feature_unique_id)
);

CREATE TABLE IF NOT EXISTS bioprotect.pu_feature_amounts (
    project_id          INTEGER NOT NULL,
    feature_unique_id   INTEGER NOT NULL,
    h3_index            TEXT NOT NULL,
    amount              DOUBLE PRECISION NOT NULL CHECK (amount >= 0)
);
CREATE INDEX IF NOT EXISTS idx_pfa_project ON bioprotect.pu_feature_amounts (project_id, feature_unique_id);
CREATE INDEX IF NOT EXISTS idx_pfa_h3 ON bioprotect.pu_feature_amounts (h3_index);

-- =============================================
-- Cost Profiles
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.cost_profiles (
    id              SERIAL PRIMARY KEY,
    project_id      INTEGER NOT NULL REFERENCES bioprotect.projects(id) ON DELETE CASCADE,
    name            TEXT NOT NULL,
    description     TEXT,
    created_by      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    is_default      BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bioprotect.cost_profile_values (
    id                  SERIAL PRIMARY KEY,
    cost_profile_id     INTEGER NOT NULL REFERENCES bioprotect.cost_profiles(id) ON DELETE CASCADE,
    project_pu_id       INTEGER NOT NULL REFERENCES bioprotect.project_pus(id) ON DELETE CASCADE,
    cost                NUMERIC NOT NULL,
    status              INTEGER DEFAULT 0  -- 0=default, 1=locked_in, 2=locked_out
);
CREATE INDEX IF NOT EXISTS idx_cpv_profile ON bioprotect.cost_profile_values (cost_profile_id);
CREATE INDEX IF NOT EXISTS idx_cpv_pu ON bioprotect.cost_profile_values (project_pu_id);

-- Add FK from projects to cost_profiles
ALTER TABLE bioprotect.projects
    ADD CONSTRAINT fk_active_cost_profile
    FOREIGN KEY (active_cost_profile_id)
    REFERENCES bioprotect.cost_profiles(id)
    ON DELETE SET NULL;

-- =============================================
-- Activities and Pressures (Cumulative Impact)
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.metadata_activities (
    id              SERIAL PRIMARY KEY,
    alias           TEXT,
    description     TEXT,
    source          TEXT,
    created_by      TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    tilesetid       TEXT,
    project_id      INTEGER,
    upload_type     TEXT DEFAULT 'raster' CHECK (upload_type IN ('raster','shapefile')),
    preprocessed    BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS bioprotect.pressures (
    id              SERIAL PRIMARY KEY,
    activity_id     INTEGER NOT NULL REFERENCES bioprotect.metadata_activities(id) ON DELETE CASCADE,
    pressuretitle   TEXT NOT NULL,
    rppscore        NUMERIC,
    geometry        GEOMETRY(MultiPolygon, 4326),
    rank            INTEGER
);
CREATE INDEX IF NOT EXISTS idx_pressures_geom ON bioprotect.pressures USING GIST (geometry);
CREATE INDEX IF NOT EXISTS idx_pressures_activity ON bioprotect.pressures (activity_id);

CREATE TABLE IF NOT EXISTS bioprotect.sensitivity_matrix (
    id                  SERIAL PRIMARY KEY,
    eunis_code          TEXT NOT NULL,
    pressure            TEXT NOT NULL,
    sensitivity_score   NUMERIC NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_sensitivity_eunis ON bioprotect.sensitivity_matrix (eunis_code);
CREATE INDEX IF NOT EXISTS idx_sensitivity_pressure ON bioprotect.sensitivity_matrix (pressure);

-- =============================================
-- Prioritizr (Conservation Planning Optimizer)
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.prioritizr_runs (
    id              BIGSERIAL PRIMARY KEY,
    project_id      INTEGER NOT NULL REFERENCES bioprotect.projects(id),
    created_by      INTEGER,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    status          TEXT DEFAULT 'queued',
    params          JSONB DEFAULT '{}',
    input_table     TEXT,
    feature_cols    TEXT[],
    error           TEXT,
    label           TEXT,
    resolved_config JSONB,
    feature_map     JSONB
);

CREATE TABLE IF NOT EXISTS bioprotect.prioritizr_run_results (
    run_id      BIGINT NOT NULL REFERENCES bioprotect.prioritizr_runs(id) ON DELETE CASCADE,
    h3_index    TEXT NOT NULL,
    solution    INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_prr_run ON bioprotect.prioritizr_run_results (run_id);

CREATE TABLE IF NOT EXISTS bioprotect.prioritizr_run_logs (
    id      BIGSERIAL PRIMARY KEY,
    run_id  BIGINT NOT NULL REFERENCES bioprotect.prioritizr_runs(id) ON DELETE CASCADE,
    ts      TIMESTAMPTZ DEFAULT NOW(),
    stream  TEXT NOT NULL,
    message TEXT NOT NULL
);

-- =============================================
-- Boundary Matrix (precomputed H3 adjacency)
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.grid_boundary_edges (
    planning_unit_id    INT NOT NULL,
    h3_a                TEXT NOT NULL,
    h3_b                TEXT NOT NULL,
    boundary            DOUBLE PRECISION NOT NULL DEFAULT 1.0,
    PRIMARY KEY (planning_unit_id, h3_a, h3_b)
);
CREATE INDEX IF NOT EXISTS idx_grid_boundary_edges_grid
    ON bioprotect.grid_boundary_edges (planning_unit_id);

-- =============================================
-- Migration Tracking
-- =============================================
CREATE TABLE IF NOT EXISTS bioprotect.schema_migrations (
    version     TEXT PRIMARY KEY,
    name        TEXT NOT NULL,
    applied_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SQL
```

### 2.4 Deploy Functions and Mark Baseline

```bash
cd server

# Deploy all 30 database functions
PGPASSWORD=postgres python db/migrate.py functions

# Mark the baseline migration as applied
PGPASSWORD=postgres python db/migrate.py migrate
```

### 2.5 Verify Installation

```bash
PGPASSWORD=postgres python db/migrate.py status

# Expected output:
# Version    Status       Name
# ------------------------------------------------------------
# 0001       applied      baseline
#
# 1 applied, 0 pending.
```

---

## 3. Updating an Existing Database

### 3.1 Routine Function Updates

When function logic changes (e.g., fixing a query, adding a parameter default), just redeploy:

```bash
cd server
PGPASSWORD=postgres python db/migrate.py functions
```

This is safe to run anytime — all functions use `CREATE OR REPLACE`.

### 3.2 Schema Migrations

When the schema changes (new table, new column, data transforms), create a migration:

```bash
# 1. Create migration file
PGPASSWORD=postgres python db/migrate.py new "add_run_label_column"
# → Creates db/migrations/0002_add_run_label_column.sql

# 2. Edit the migration file
# Example content:
#   ALTER TABLE bioprotect.prioritizr_runs
#     ADD COLUMN IF NOT EXISTS label TEXT;

# 3. Apply it
PGPASSWORD=postgres python db/migrate.py migrate
```

### 3.3 Full Deploy (Recommended)

Deploy everything in the correct order: functions, views, seeds, then migrations.

```bash
PGPASSWORD=postgres python db/migrate.py deploy
```

### 3.4 Merging with Existing Data

When migrating a database that has existing data:

**Pattern 1: Adding a column**
```sql
-- Safe: IF NOT EXISTS prevents errors on re-run
ALTER TABLE bioprotect.projects
  ADD COLUMN IF NOT EXISTS active_cost_profile_id INTEGER;
```

**Pattern 2: Adding data with conflict handling**
```sql
-- Safe: ON CONFLICT prevents duplicate errors
INSERT INTO bioprotect.sensitivity_matrix (eunis_code, pressure, sensitivity_score)
VALUES ('Maerl', 'Abrasion', 0.8)
ON CONFLICT DO NOTHING;
```

**Pattern 3: Changing a column type**
```sql
-- Safe: wrap in a DO block to check first
DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
    WHERE table_schema = 'bioprotect'
      AND table_name = 'my_table'
      AND column_name = 'my_col'
      AND data_type = 'text'
  ) THEN
    ALTER TABLE bioprotect.my_table
      ALTER COLUMN my_col TYPE INTEGER USING my_col::INTEGER;
  END IF;
END $$;
```

**Pattern 4: Renaming/replacing a function signature**
```sql
-- If args or return type changes, DROP first then CREATE
DROP FUNCTION IF EXISTS bioprotect.my_func(INTEGER);
-- Then db/functions/my_func.sql will recreate it via deploy
```

---

## 4. Schema Reference

### Core Tables

| Table | Purpose | Size (approx) |
|-------|---------|---------------|
| `projects` | Project metadata, active cost profile link | Small |
| `project_pus` | Links H3 hexes to projects | 1 GB |
| `project_features` | Per-project feature targets (0-100) and SPF | Small |
| `users` / `user_projects` | Authentication and project ownership | Small |

### Spatial Data

| Table | Purpose | Size (approx) |
|-------|---------|---------------|
| `h3_cells` | H3 hex geometries for all grids | 3.3 GB |
| `metadata_planning_units` | Planning grid metadata | Small |
| `metadata_interest_features` | Conservation feature metadata | Small |
| `pu_feature_amounts` | Feature coverage per planning unit | 6 MB |
| `f_*` tables | Individual feature geometries (one per feature) | Varies |

### Cost System

| Table | Purpose | Size (approx) |
|-------|---------|---------------|
| `cost_profiles` | Cost profile metadata (name, description) | Small |
| `cost_profile_values` | Per-PU cost and lock status | 400 MB |

**PU Status Values:**
- `0` = default (normal)
- `1` = locked in (blue in UI)
- `2` = locked out (red in UI)

### Cumulative Impact

| Table | Purpose |
|-------|---------|
| `metadata_activities` | Uploaded activity metadata |
| `activity_*` tables | Individual activity geometries |
| `pressures` | Activity-pressure pairs with geometry |
| `sensitivity_matrix` | 936 rows: habitat x pressure sensitivity scores |

### Prioritizr

| Table | Purpose |
|-------|---------|
| `prioritizr_runs` | Run metadata, params, status |
| `prioritizr_run_results` | Solution per hex (0 or 1) |
| `prioritizr_run_logs` | Streamed R output per run |
| `prioritizr_input_run_*` | Temporary wide tables (UNLOGGED, per-run) |
| `grid_boundary_edges` | Precomputed H3 adjacency per grid |

### Key Relationships

```
projects
  ├── active_cost_profile_id ──► cost_profiles
  ├── planning_unit_id ──► metadata_planning_units
  │
  ├── project_pus (H3 hexes)
  │     └── cost_profile_values (cost + status per hex)
  │
  ├── project_features (targets per feature)
  │     └── pu_feature_amounts (coverage per hex)
  │
  └── prioritizr_runs
        ├── prioritizr_run_results
        └── prioritizr_run_logs

metadata_planning_units
  └── grid_boundary_edges (precomputed H3 adjacency)

metadata_activities
  └── pressures (one per activity-pressure pair)
```

---

## 5. Functions Reference

### Cumulative Impact
| Function | Purpose |
|----------|---------|
| `run_cumulative_impact(project_id, activity_ids[], profile_name, description, user)` | Compute CI and create cost profile |
| `run_impact_pipeline(project_id, activity_ids[], ...)` | Create pressures + run CI |
| `create_pressures_from_activity(activity_id)` | Generate pressure rows from activity |
| `aggregate_feature_stats(project_id)` | Aggregate feature statistics |

### Prioritizr
| Function | Purpose |
|----------|---------|
| `prepare_prioritizr_input(run_id)` | Build wide input table with features, cost, locked_in/out |
| `get_prioritizr_run_config(run_id)` | Return run config including feature_targets_json |

### Boundary Matrix
| Function | Purpose |
|----------|---------|
| `populate_grid_boundary_edges(planning_unit_id)` | One-time precompute per grid |
| `get_project_boundary_edges(project_id)` | Fast precomputed edge lookup |
| `get_project_h3_adjacency(project_id)` | Runtime fallback (slower for large grids) |

### Planning Units
| Function | Purpose |
|----------|---------|
| `get_planning_units_for_project(project_id)` | Returns PU layer with cost from active profile |
| `set_active_profile_pu_statuses(project_id, status1_h3[], status2_h3[], status3_h3[])` | Set locked_in/out/status3 |
| `get_pu_grids()` | List available planning grids |
| `get_project_pus(project_id)` | Get planning units for a project |

### Features
| Function | Purpose |
|----------|---------|
| `get_project_features(project_id)` | Return features with targets |
| `update_project_feature(...)` | Update single feature properties |
| `insert_feature_pu_amounts(...)` | Insert feature coverage data |
| `clear_feature_data(project_id, feature_id)` | Remove feature preprocessing data |

### Project Management
| Function | Purpose |
|----------|---------|
| `get_projects_for_user(user_id)` | List accessible projects |
| `gap_analysis(...)` | Run gap analysis |

---

## 6. Migration System

### Directory Structure

```
server/db/
├── migrate.py          # Migration runner CLI
├── deploy_all.sh       # Shell wrapper
├── DATABASE.md         # This file
├── README.md           # Quick reference
├── migrations/         # Sequential, run-once schema changes
│   └── 0001_baseline.sql
├── functions/          # 30 idempotent CREATE OR REPLACE files
│   ├── run_cumulative_impact.sql
│   ├── prepare_prioritizr_input.sql
│   └── ...
├── views/              # Materialized view definitions
└── seeds/              # Reference data (INSERT ON CONFLICT)
```

### Commands

| Command | Purpose |
|---------|---------|
| `python db/migrate.py status` | Show applied/pending migrations |
| `python db/migrate.py migrate` | Run pending migrations |
| `python db/migrate.py deploy` | Full deploy: functions + views + seeds + migrations |
| `python db/migrate.py new "desc"` | Create next migration file |
| `python db/migrate.py functions` | Deploy functions only |
| `bash db/deploy_all.sh dump-schema` | Generate schema dump |

### Rules

1. **Never edit** a migration after it has been applied to any environment
2. **Never delete** a migration file
3. Migrations run inside an implicit transaction
4. Use `IF NOT EXISTS` / `IF EXISTS` for defensive SQL
5. Function changes go in `db/functions/`, not in migrations
6. Test migrations locally before applying to production

---

## 7. Configuration

### Environment Variables

The application reads from `server/.env.local`:

```ini
db_name=bioprotect
db_host=localhost
db_user=postgres
db_pass=postgres
port=5432
```

The migration runner reads standard PostgreSQL env vars:

```bash
export PGHOST=localhost
export PGPORT=5432
export PGDATABASE=bioprotect
export PGUSER=postgres
export PGPASSWORD=postgres
```

The R scripts also use these `PG*` env vars for direct database access.

### Connection Pools

| Pool | Library | Min | Max | Timeout | Used By |
|------|---------|-----|-----|---------|---------|
| Async | aiopg | 2 | 10 | 60s (acquisition) | Tornado handlers |
| Sync | SQLAlchemy | 3 | 13 | 30s | GeoDataFrame ops |
| Direct | RPostgres | 1 | 1 | None | R prioritizr scripts |

---

## 8. Backup and Restore

### Full Backup

```bash
# Schema + data (exclude temporary prioritizr input tables)
pg_dump -h localhost -U postgres -d bioprotect \
  --schema=bioprotect \
  --exclude-table='bioprotect.prioritizr_input_run_*' \
  -Fc -f bioprotect_$(date +%Y%m%d).dump

# Schema only (for reference)
bash db/deploy_all.sh dump-schema
```

### Restore to Fresh Database

```bash
# 1. Create empty database with extensions (see Section 2.2)
# 2. Restore data
pg_restore -h localhost -U postgres -d bioprotect \
  --no-owner --no-privileges \
  bioprotect_20260409.dump

# 3. Deploy latest functions (in case dump had older versions)
cd server
PGPASSWORD=postgres python db/migrate.py deploy
```

### Restore to Existing Database (Merge)

```bash
# Restore with --data-only to preserve existing schema
pg_restore -h localhost -U postgres -d bioprotect \
  --data-only --disable-triggers \
  --no-owner --no-privileges \
  bioprotect_20260409.dump

# Then redeploy functions
PGPASSWORD=postgres python db/migrate.py deploy
```

---

## 9. Troubleshooting

### Common Issues

**"h3_grid_ring does not exist"**
This h3-pg installation only has `h3_grid_disk` and `h3_grid_ring_unsafe`. Use:
```sql
-- Instead of h3_grid_ring(h3index, 1):
h3_grid_disk(h3index, 1)  -- returns center + 6 neighbors
-- Filter out center:
WHERE n::text <> source.h3_index
```

**"timeout expired" on prioritizr run**
The aiopg pool timeout (60s) is for connection acquisition, not query execution. If all 10 connections are busy, new queries wait up to 60s. Check for:
```sql
SELECT pid, state, now() - query_start as duration, left(query, 80)
FROM pg_stat_activity
WHERE state = 'active' AND query NOT LIKE '%pg_stat%';
```

**Spatial queries timing out**
Large pressure geometries (300K+ vertices) cause slow ST_Intersection. Use:
- `ST_Contains(ST_Simplify(geom, 0.0001), ST_Centroid(hex))` instead of `ST_Intersection`
- See `run_cumulative_impact` function for the pattern

**"Cannot delete: cost profile is active"**
A cost profile that is set as `active_cost_profile_id` on any project cannot be deleted. Deactivate it first:
```sql
UPDATE bioprotect.projects SET active_cost_profile_id = NULL WHERE active_cost_profile_id = <id>;
```

**Stale prioritizr input tables**
Temporary `prioritizr_input_run_*` tables accumulate. Clean up:
```sql
DO $$
DECLARE r RECORD;
BEGIN
  FOR r IN SELECT tablename FROM pg_tables
    WHERE schemaname = 'bioprotect' AND tablename LIKE 'prioritizr_input_run_%'
  LOOP
    EXECUTE format('DROP TABLE IF EXISTS bioprotect.%I', r.tablename);
  END LOOP;
END $$;
```

### Useful Diagnostic Queries

```sql
-- Check extension versions
SELECT extname, extversion FROM pg_extension ORDER BY extname;

-- Table sizes
SELECT relname, pg_size_pretty(pg_total_relation_size(c.oid))
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE n.nspname = 'bioprotect' AND c.relkind = 'r'
ORDER BY pg_total_relation_size(c.oid) DESC LIMIT 20;

-- Active queries
SELECT pid, state, now()-query_start as duration, left(query,100)
FROM pg_stat_activity WHERE state='active' AND query NOT LIKE '%pg_stat%';

-- Migration status
SELECT * FROM bioprotect.schema_migrations ORDER BY version;

-- Check boundary edge precomputation
SELECT planning_unit_id, COUNT(*) as edges
FROM bioprotect.grid_boundary_edges GROUP BY 1 ORDER BY 2 DESC;
```
