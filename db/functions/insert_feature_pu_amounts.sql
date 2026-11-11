-- Feature amount per grid cell, for the cells with h3_index in [p_lo, p_hi]
-- (NULLs = every cell). Polygon features read bioprotect.feature_pieces, so
-- build them first (feature_piece_batches / build_feature_pieces).
CREATE OR REPLACE FUNCTION bioprotect.insert_feature_pu_amounts(p_project_id integer, p_feature_id integer, p_planning_unit_id integer, p_feature_class text, p_geom_type text, p_lo text, p_hi text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_grid_table TEXT;
    v_area_srid  INTEGER := bioprotect.project_area_srid(p_project_id);
    v_range      TEXT := '';
BEGIN
    SELECT feature_class_name
      INTO v_grid_table
      FROM bioprotect.metadata_planning_units
     WHERE unique_id = p_planning_unit_id;

    IF v_grid_table IS NULL THEN
        RAISE EXCEPTION 'No planning unit table found for ID %', p_planning_unit_id;
    END IF;

    IF p_lo IS NOT NULL THEN
        v_range := format(' AND grid.h3_index::text BETWEEN %L AND %L', p_lo, p_hi);
    END IF;

    IF p_geom_type = 'ST_Point' THEN
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts
                (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index, SUM(feat.value)
            FROM bioprotect.%I AS grid
            JOIN bioprotect.%I AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            WHERE true %s
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_grid_table, p_feature_class, v_range);

    ELSE
        -- Polygon features: store coverage in km², measured in the project
        -- CRS (geography when that is geographic; see area_km2.sql). km² is
        -- the app-wide canonical unit for feature amounts — keeps numbers
        -- readable and avoids prioritizr's 1e6 presolve numerical cap.
        -- The union is needed: uploads can overlap (CSO has 10,920
        -- overlapping polygon pairs), so summing piece areas double-counts.
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index,
                bioprotect.area_km2(
                    ST_CollectionExtract(
                        ST_Union(
                            ST_Intersection(grid.geometry, feat.geometry)),
                        3
                    ),
                    %L::integer
                ) AS amount
            FROM bioprotect.%I AS grid
            JOIN bioprotect.feature_pieces AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            WHERE feat.feature_unique_id = %s %s
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_area_srid, v_grid_table, p_feature_id, v_range);
    END IF;
END;
$function$
