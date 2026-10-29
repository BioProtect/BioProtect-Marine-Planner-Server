-- Migration 0014: drop legacy columns, tables and functions
-- Created: 2026-10-29
--
-- h3_cells.geom: a duplicate of h3_cells.geometry (equal wherever both were
-- set, NULL for some grids, SRID 0). Nothing reads it; the GIST index moves
-- to geometry, which every query filters on.
--
-- eez_simplified_1km: only read by the legacy Marxan planning_grid()
-- overloads, which nothing calls.
--
-- metadata_impacts: read by /server/getAllImpacts, whose result the frontend
-- discarded. Never written to. Handler and calls removed.
--
-- gap_analysis(): Marxan-era, reads marxan.* tables and an `oid` column that
-- no longer exist; nothing calls it.

CREATE INDEX IF NOT EXISTS h3_cells_geometry_gix
  ON bioprotect.h3_cells USING gist (geometry);

ALTER TABLE bioprotect.h3_cells DROP COLUMN IF EXISTS geom;

DROP FUNCTION IF EXISTS bioprotect.planning_grid(double precision, text, text, text, text);
DROP FUNCTION IF EXISTS bioprotect.gap_analysis(text, integer[], text, text);

DROP TABLE IF EXISTS bioprotect.eez_simplified_1km;
DROP TABLE IF EXISTS bioprotect.metadata_impacts;
