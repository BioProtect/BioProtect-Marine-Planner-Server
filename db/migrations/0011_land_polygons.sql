-- Migration 0011: bioprotect.land (OSM land polygons)
-- Created: 2026-10-19
--
-- Planning grids drawn on the map drop every hexagon whose centroid falls in
-- this table (planning_unit_websocket_handler.py). Uploaded grids are not
-- filtered.
--
-- This only creates the empty table. The data (~900 MB download, ~1.4 GB in
-- the DB, ~1.36M rows) is loaded by db/load_land.sh. Until it is loaded,
-- drawing a grid fails with a message saying so.
--
-- Data is OpenStreetMap, ODbL: credit "© OpenStreetMap contributors" and ship
-- the licence notice with any self-hosted bundle.
--
-- Updates:
--   Tables  : land (new)
--   Indexes : land_geom_gist (new)
-- ============================================================

CREATE TABLE IF NOT EXISTS bioprotect.land (
    id   bigserial PRIMARY KEY,
    geom geometry(Polygon, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS land_geom_gist
    ON bioprotect.land USING gist (geom);
