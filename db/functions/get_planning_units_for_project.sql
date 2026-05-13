CREATE OR REPLACE FUNCTION bioprotect.get_planning_units_for_project(p_project_id integer)
 RETURNS TABLE(h3_index text, cost numeric, status integer)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
    v_active_profile_id INT;
    v_domain            TEXT;
    v_resolution        INT;
BEGIN
    -- 1. Get project's resolution + active cost profile
    SELECT default_resolution, active_cost_profile_id
    INTO v_resolution, v_active_profile_id
    FROM bioprotect.projects
    WHERE id = p_project_id;

    -- 2. Get domain/project area label for matching against h3_cells
    SELECT LOWER(TRIM(split_part(mpu.alias, ' (', 1)))
    INTO v_domain
    FROM bioprotect.projects p
    JOIN bioprotect.metadata_planning_units mpu
      ON p.planning_unit_id = mpu.unique_id
    WHERE p.id = p_project_id;

    -- 3. If no active profile, try default cost profile for project
    IF v_active_profile_id IS NULL THEN
        SELECT id
        INTO v_active_profile_id
        FROM bioprotect.cost_profiles
        WHERE project_id = p_project_id
          AND is_default = TRUE
        ORDER BY id
        LIMIT 1;
    END IF;

    -- 4. If still no profile, fallback: cost = 0, status = 0
    IF v_active_profile_id IS NULL THEN
        RETURN QUERY
        SELECT
            pp.h3_index,
            0::NUMERIC    AS cost,
            0::INTEGER    AS status
        FROM bioprotect.project_pus pp
        JOIN bioprotect.h3_cells hc
          ON hc.h3_index = pp.h3_index
        WHERE pp.project_id = p_project_id
          AND LOWER(TRIM(hc.project_area)) = v_domain;
        RETURN;
    END IF;

    -- 5. Normal path: use active (or default) cost profile
    RETURN QUERY
    SELECT
        pp.h3_index,
        cpv.cost,
        cpv.status
    FROM bioprotect.cost_profile_values cpv
    JOIN bioprotect.project_pus pp
      ON pp.id = cpv.project_pu_id
    JOIN bioprotect.h3_cells hc
      ON hc.h3_index = pp.h3_index
    WHERE cpv.cost_profile_id = v_active_profile_id
      AND pp.project_id = p_project_id
      AND LOWER(TRIM(hc.project_area)) = v_domain;

END;
$function$

