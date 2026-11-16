# BioProtect Server

Backend for BioProtect, a marine spatial conservation planning tool (Prioritizr).
A Python Tornado app (REST + WebSockets) in front of PostgreSQL/PostGIS. Most of the spatial work is done by
SQL functions in the database, and the optimisation by an R script (`services/run_prioritzr_v2.R`, Prioritizr + CBC).
Map tiles are served straight from the database by [Martin](https://martin.maplibre.org/).

The web client is the separate [BioProtect-Marine-Planner-UI](https://github.com/BioProtect/BioProtect-Marine-Planner-UI) repo.

## Requirements

- Python 3.12
- PostgreSQL 15+ with the PostGIS, `postgis_raster` and [h3-pg](https://github.com/zachasme/h3-pg) extensions
- GDAL/OGR (`/usr/bin/ogr2ogr`), GEOS and PROJ
- R with Prioritizr and the CBC solver (see [R dependencies](#r-dependencies))
- Martin 0.15+ (tile server)

On Ubuntu (replace `16` with your Postgres major version):

```
sudo apt install -y postgresql-16-postgis-3 postgresql-16-h3 gdal-bin libgdal-dev python3-venv
```

## Setup

### 1. Python

```
cd server
python3 -m venv .venv
.venv/bin/pip install GDAL==$(gdal-config --version)
.venv/bin/pip install -r requirements.txt
```

GDAL has to match the system `libgdal`, so it's installed first.

### 2. Configuration

```
cp .env.example .env.local
```

Fill in `.env.local`. The database keys and the three secrets (`cookie_secret`, `access_token`, `refresh_token`,
any long random strings) are required. Startup fails fast if the database keys are missing.
Any key can be overridden with a `BIOPROTECT_<KEY>` environment variable (e.g. `BIOPROTECT_DB_NAME`).

The `config.json` / `config_prod.json` files are legacy Marxan config and are not used.

### 3. Database

Create an empty database, load the schema, then apply any newer migrations and the SQL functions:

```
createdb bioprotect
psql -d bioprotect -v ON_ERROR_STOP=1 -f db/install/01_schema.sql
.venv/bin/python db/migrate.py deploy
```

`01_schema.sql` is structure only (it creates the extensions too). Optional extras:

- **Reference data** (`db/install/02_seed.sql`, not in git): the UK/EU sensitivity matrix, the Marine Pressures
  and Activities Database (PAD) and the JNCC sensitivities. Only cumulative impact uses it. Without it the server
  runs fine and cumulative impact comes out empty. Load it after `01_schema.sql` if you have it.
- **Land polygons**, for removing land from planning grids drawn on the map: `bash db/load_land.sh`
  (downloads about 900 MB of OSM data and takes tens of minutes; © OpenStreetMap contributors, ODbL).
- **WDPA protected areas**, for the protected-areas reference layer: `bash db/load_wdpa.sh <download>`.
  The WDPA licence forbids redistributing it, so download it yourself from Protected Planet.

Database details (migrations, backups, troubleshooting) are in [`db/DATABASE.md`](db/DATABASE.md).

### 4. First user

There's no sign-up page for an empty install. Create an Admin from `server/`:

```
.venv/bin/python -m users.create_user admin you@example.org --role Admin   # prompts for the password
```

## Running (development)

```
.venv/bin/python app.py                  # API on server_port (default 8080)
martin -c tileserver/config.yaml         # tiles on :3000
```

On `localhost` the client expects the API on port 8080 and Martin on 3000, so keep those ports in development.
Martin reads the database from `DATABASE_URL` (e.g. `postgres://user:pass@localhost/bioprotect`). The config
publishes only the tile functions (`grid_tiles`, `layer_tiles`, `reference_tiles`, `wdpa_tiles`), not every table.

## Production

Three services behind nginx on one origin: the app (systemd), Martin (systemd user unit) and the built client
(static files). The paths below assume the checkout at `/opt/bioprotect-server` and the client at
`/opt/bioprotect-client`; change them in the unit files if yours differ.

1. Do the [Setup](#setup) steps in `/opt/bioprotect-server`.
2. **App:** edit `deploy/bioprotect.service` (`User`, and the `PG*` variables: the Prioritizr R script
   connects with those, not with `.env.local`), then:
   ```
   sudo cp deploy/bioprotect.service /etc/systemd/system/
   sudo systemctl daemon-reload && sudo systemctl enable --now bioprotect
   ```
3. **Martin:** set `DATABASE_URL` in `tileserver/martin.service` if the default
   (`postgres://postgres:postgres@localhost/bioprotect`) doesn't fit, then, as the user that runs it:
   ```
   cp tileserver/martin.service ~/.config/systemd/user/
   systemctl --user daemon-reload && systemctl --user enable --now martin
   sudo loginctl enable-linger $USER    # keep it running after logout
   ```
4. **nginx:** start from `deploy/nginx.conf` (replace the server name, then add HTTPS). It serves the client
   and proxies `/server/` to the app and `/tiles/` to Martin. The trailing slash on the `/tiles/` `proxy_pass`
   matters: it strips the prefix, and `base_path: /tiles` in `tileserver/config.yaml` adds it back to the
   tile URLs Martin hands out.
5. **Client:** build the [UI repo](https://github.com/BioProtect/BioProtect-Marine-Planner-UI) and copy
   `build/` into `/opt/bioprotect-client`.
6. In `.env.local`, set `permitted_domains` to the site's origin (e.g. `https://example.org`).

**Updating:** `git pull`, `.venv/bin/python db/migrate.py deploy`, `sudo systemctl restart bioprotect`.
Restart Martin (`systemctl --user restart martin`) only when a migration adds a tile function or changes one's
SQL: it finds functions at startup and caches tiles it has served. New grids, features and layers don't need it.

Martin 0.19 is what production runs. Older versions ignore `base_path`, so test a Martin upgrade or downgrade
against the `/tiles/` proxy.

## Tests

```
.venv/bin/python -m pytest              # fast set
.venv/bin/python -m pytest -m slow      # Prioritizr/R runs and large imports
.venv/bin/python -m pytest --cov=. --cov-report=term
```

The tests build a throwaway `bioprotect_test` database from `db/install/` + `migrate.py deploy` on every run
(so they need both install files) and point the app at it and at temp import/export folders.
Your own database is never touched.

## Layout

- `app.py` - route table, handler wiring, startup/shutdown
- `handlers/` - one handler per domain; `*_websocket_handler.py` stream progress for long jobs
- `services/` - business logic, plus the Prioritizr R script
- `db/functions/` - the SQL functions (the source of record; deployed by `migrate.py`)
- `db/migrations/` - numbered schema migrations
- `db/install/` - the empty-install schema, regenerated by `bash db/export_install.sh`
- `tileserver/` - Martin config and systemd unit
- `tests/` - pytest suite

## R dependencies

```
sudo apt update
sudo apt install -y \
    r-base \
    libpq-dev \
    libssl-dev \
    libcurl4-openssl-dev \
    libxml2-dev \
    libudunits2-dev \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    coinor-libcbc-dev \
    coinor-libcbc3 \
    coinor-cbc \
    cmake \
    g++ \
    make \
    pkg-config
```

```
install.packages(c("sf","DBI","RPostgres","prioritizr","Matrix","jsonlite","highs"),
                 repos="https://cloud.r-project.org")
```

`rcbc` isn't on CRAN; install it from GitHub, see https://github.com/dirkschumacher/rcbc.
Prioritizr detects and uses `rcbc` automatically once it's installed.
