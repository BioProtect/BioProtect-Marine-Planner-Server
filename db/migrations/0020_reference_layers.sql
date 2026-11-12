-- Migration 0020: user-uploaded reference layers
-- Created: 2026-11-11
--
-- View-only map layers users upload as zipped shapefiles, shown beside WDPA in
-- the Reference layers dialog. Nothing in the analysis reads them. Every user
-- sees every layer; the uploader or an Admin can delete one.
--
-- One shared shapes table for every layer (not a table per upload), so
-- deleting a layer is one row and one tile function serves them all.
-- Polygons are stored like wdpa_areas (0013): subdivided 'fill' pieces with
-- the attributes, and the boundary subdivided separately as 'outline', so
-- outlines show no seams. size_km is the whole source shape's area (km²) for
-- polygons or length (km) for lines, used to skip sub-pixel shapes in tiles.

CREATE TABLE IF NOT EXISTS bioprotect.reference_layers (
    id          serial PRIMARY KEY,
    name        text NOT NULL,
    description text,
    geom_type   text NOT NULL CHECK (geom_type IN ('polygon', 'line', 'point')),
    colour      text NOT NULL,
    created_by  text,
    created_at  timestamptz NOT NULL DEFAULT now()
);
CREATE UNIQUE INDEX IF NOT EXISTS reference_layers_name_key
    ON bioprotect.reference_layers (lower(name));

CREATE TABLE IF NOT EXISTS bioprotect.reference_layer_shapes (
    layer_id   integer NOT NULL
        REFERENCES bioprotect.reference_layers (id) ON DELETE CASCADE,
    part       text NOT NULL CHECK (part IN ('fill', 'outline', 'line', 'point')),
    attributes jsonb,
    size_km    double precision NOT NULL DEFAULT 0,
    geom       geometry(Geometry, 4326) NOT NULL
);
CREATE INDEX IF NOT EXISTS reference_layer_shapes_layer_idx
    ON bioprotect.reference_layer_shapes (layer_id);
CREATE INDEX IF NOT EXISTS reference_layer_shapes_geom_gix
    ON bioprotect.reference_layer_shapes USING gist (geom);
