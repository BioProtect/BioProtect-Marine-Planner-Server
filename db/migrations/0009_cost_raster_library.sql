-- Migration 0009: cost raster library (raster-free value cache)
-- Created: 2026-09-26
--
-- Lets an uploaded cost raster be reused across projects, planning grids and
-- resolutions WITHOUT keeping the raster file on the server.
--
-- On upload we extract the zonal stat against every h3_cell that intersects
-- the raster's footprint - all resolutions, all project areas - store the raw
-- per-hex values here, then delete the file. Creating a cost profile for any
-- project afterwards is a join against project_pus, no raster IO.
--
-- Visibility lives on cost_rasters; raster_hex_values is reachable only
-- through the FK, so the owner/visibility check on the parent is the whole
-- access rule. Default is private: a cache is consulted implicitly, so
-- shared-by-default would silently leak one user's data into another's
-- project. Columns are written now and enforced when auth lands
-- (DISABLE_SECURITY is still true) so there is no backfill later.
--
-- Updates:
--   Tables : cost_rasters (new), raster_hex_values (new)
-- ============================================================


-- ============================================================
-- 1. cost_rasters - one row per extracted raster. No file kept.
-- ============================================================
CREATE TABLE IF NOT EXISTS bioprotect.cost_rasters (
    id              SERIAL PRIMARY KEY,
    name            TEXT NOT NULL,
    description     TEXT DEFAULT '',
    source_filename TEXT,
    band            INTEGER NOT NULL DEFAULT 1,
    stat            TEXT NOT NULL DEFAULT 'mean',
    -- sha256 of the uploaded file, for owner-scoped dedupe on re-upload
    checksum        TEXT,
    -- footprint in WGS84, so we can tell the user what a cached raster covers
    bounds          GEOMETRY(Polygon, 4326),
    owner_id        INTEGER REFERENCES bioprotect.users(id) ON DELETE SET NULL,
    visibility      TEXT NOT NULL DEFAULT 'private'
                    CHECK (visibility IN ('private', 'shared')),
    hex_count       INTEGER NOT NULL DEFAULT 0,
    date_created    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Dedupe key: the same file re-uploaded by the same user for the same band
-- and stat is the same extraction. Scoped to the owner deliberately - a
-- global match would tell one user that another holds their file.
CREATE UNIQUE INDEX IF NOT EXISTS idx_cost_rasters_owner_checksum
    ON bioprotect.cost_rasters (owner_id, checksum, band, stat)
    WHERE checksum IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_cost_rasters_owner
    ON bioprotect.cost_rasters (owner_id);

CREATE INDEX IF NOT EXISTS idx_cost_rasters_bounds
    ON bioprotect.cost_rasters USING GIST (bounds);


-- ============================================================
-- 2. raster_hex_values - raw (un-normalised) stat per hex.
-- ============================================================
-- Raw, not normalised: min/max depend on which hex set you are normalising
-- over, so the transform has to happen at profile-creation time.
--
-- Only covered hexes are stored. An absent row means "raster did not cover
-- this hex", which is exactly what the fill_strategy handles downstream.
CREATE TABLE IF NOT EXISTS bioprotect.raster_hex_values (
    raster_id  INTEGER NOT NULL
        REFERENCES bioprotect.cost_rasters(id) ON DELETE CASCADE,
    h3_index   TEXT NOT NULL,
    value      DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (raster_id, h3_index)
);

-- Lookups are always "values for this raster, for these project hexes".
CREATE INDEX IF NOT EXISTS idx_raster_hex_values_h3
    ON bioprotect.raster_hex_values (h3_index);


-- ============================================================
-- 3. cost_profiles -> source raster, so the UI can say where a
--    profile came from and warn when its source is private.
-- ============================================================
ALTER TABLE bioprotect.cost_profiles
    ADD COLUMN IF NOT EXISTS source_raster_id INTEGER
        REFERENCES bioprotect.cost_rasters(id) ON DELETE SET NULL;
