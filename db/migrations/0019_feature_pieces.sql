-- Migration 0019: shared feature_pieces table for batched preprocessing
-- Created: 2026-11-11
--
-- insert_feature_pu_amounts used to ST_Subdivide the whole feature into a temp
-- table on every call, then intersect every cell in one query. For the CSO
-- Sublittoral Sediments layer (58k polygons) that single query is ~42-50 s on
-- dev and over the 60 s pool cap on prod. Now the pieces are built once per
-- feature (build_feature_pieces, batched by source id) and reused across
-- projects, and the per-cell work runs in h3_index ranges (feature_cell_batches)
-- so each call stays short.
--
-- source_id is the feature row the piece came from. Pieces are built in
-- ascending id order, so "max(source_id) = the table's max(id)" means the last
-- batch committed; a feature that gains rows (Engage merge) is rebuilt.

CREATE TABLE IF NOT EXISTS bioprotect.feature_pieces (
    feature_unique_id integer NOT NULL
        REFERENCES bioprotect.metadata_interest_features (unique_id) ON DELETE CASCADE,
    source_id integer NOT NULL,
    geometry geometry NOT NULL
);
CREATE INDEX IF NOT EXISTS feature_pieces_feature_idx
    ON bioprotect.feature_pieces (feature_unique_id, source_id);
CREATE INDEX IF NOT EXISTS feature_pieces_geometry_gix
    ON bioprotect.feature_pieces USING gist (geometry);

-- replaced by the 7-argument version (h3 range) in db/functions/
DROP FUNCTION IF EXISTS bioprotect.insert_feature_pu_amounts(integer, integer, integer, text, text);
