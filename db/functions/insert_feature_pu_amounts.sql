CREATE OR REPLACE FUNCTION bioprotect.insert_feature_pu_amounts(p_project_id integer, p_feature_id integer, p_planning_unit_id integer, p_feature_class text, p_geom_type text)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_grid_table TEXT;
BEGIN
    -- Resolve grid table name from metadata_planning_units
    SELECT feature_class_name
      INTO v_grid_table
      FROM bioprotect.metadata_planning_units
     WHERE unique_id = p_planning_unit_id;

    IF v_grid_table IS NULL THEN
        RAISE EXCEPTION 'No planning unit table found for ID %', p_planning_unit_id;
    END IF;

    RAISE NOTICE 'Using grid table: %, feature table: %', v_grid_table, p_feature_class;

    IF p_geom_type = 'ST_Point' THEN
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts
                (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index, SUM(feat.value)
            FROM bioprotect.%I AS grid
            JOIN bioprotect.%I AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_grid_table, p_feature_class);  -- ✅ correct order

    ELSE
        -- Polygon features: store coverage in km² (ST_Area gives m² in
        -- EPSG:3410, divide by 1e6). km² is the app-wide canonical unit
        -- for feature amounts — keeps numbers readable and avoids
        -- prioritizr's 1e6 presolve numerical cap.
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index,
                ST_Area(
                    ST_Transform(
                        ST_Union(ST_Intersection(grid.geometry, feat.geometry)),
                        3410
                    )
                ) / 1000000.0 AS amount
            FROM bioprotect.%I AS grid
            JOIN bioprotect.%I AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_grid_table, p_feature_class);
    END IF;
END;
$function$

