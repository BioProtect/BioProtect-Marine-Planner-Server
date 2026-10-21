-- Migration 0013: bioprotect.wdpa_areas (WDPA protected areas, view-only layer)
-- Created: 2026-10-21
--
-- The "Reference layers" dialog shows marine/coastal WDPA polygons on the
-- map. View-only: nothing in the analysis reads this table.
--
-- This only creates the empty tables. The data is loaded by db/load_wdpa.sh
-- from a WDPA File Geodatabase downloaded from protectedplanet.net (the
-- download needs the terms accepted in a browser). Until it is loaded, the
-- dialog says so.
--
-- Licence: WDPA terms forbid redistributing the data and require
-- attribution ("UNEP-WCMC and IUCN, Protected Planet"). Never ship it in the
-- self-hosted bundle - that org runs the load script on their own download.
--
-- Each protected area is stored twice, as ST_Subdivide'd pieces, so a tile
-- only reads small geometries:
--   part = 'fill'  polygon pieces, carrying the attributes the click info shows
--   part = 'line'  its boundary as line pieces. Outlining the fill pieces
--                  instead would draw the subdivision seams.
--
-- Updates:
--   Tables  : wdpa_areas, wdpa_release (new)
--   Indexes : wdpa_areas_geom_gist (new)
-- ============================================================

CREATE TABLE IF NOT EXISTS bioprotect.wdpa_areas (
    wdpaid    bigint   NOT NULL,
    name      text,
    desig_eng text,
    iucn_cat  text,
    status    text,
    status_yr integer,
    area_km2  double precision NOT NULL,   -- of the whole area, for low-zoom culling
    part      text     NOT NULL CHECK (part IN ('fill', 'line')),
    geom      geometry(Geometry, 4326) NOT NULL
);

CREATE INDEX IF NOT EXISTS wdpa_areas_geom_gist
    ON bioprotect.wdpa_areas USING gist (geom);

-- One row: which WDPA release is loaded (shown in the map attribution)
CREATE TABLE IF NOT EXISTS bioprotect.wdpa_release (
    release   text        NOT NULL,
    loaded_at timestamptz NOT NULL DEFAULT now()
);
