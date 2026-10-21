-- Migration 0012: per-project CRS
-- Created: 2026-10-21
--
-- A project picks its CRS (EPSG code) at creation; it cannot be changed
-- afterwards. Geometry storage stays EPSG:4326 (H3, Martin and the map all
-- depend on it). The project CRS is used for:
--   - area calculations (bioprotect.area_km2 / project_area_srid)
--   - the fallback source CRS when an uploaded file has no CRS metadata
--   - the CRS of exported shapefiles / GeoPackages
--
-- Existing projects get 4326. Behaviour change: feature amounts for 4326
-- projects are now measured on the geography ellipsoid instead of EPSG:3410
-- (deprecated EASE-Grid). Stored amounts only change when a feature is
-- preprocessed again, so old and new runs may differ slightly (<1%).
--
-- Updates:
--   Tables  : projects (crs_epsg, new column)
-- ============================================================

ALTER TABLE bioprotect.projects
    ADD COLUMN IF NOT EXISTS crs_epsg integer NOT NULL DEFAULT 4326;
