CREATE OR REPLACE FUNCTION bioprotect.get_planning_units_for_project(p_project_id integer)
 RETURNS TABLE(h3_index text, cost numeric, status integer)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
    v_active_profile_id INT;
BEGIN
    -- project_pus is filled from the grid's own h3_cells at creation, so it
    -- needs no join back to h3_cells to scope it to the grid. That join
    -- matched on LOWER(TRIM(project_area)) per row and took ~2s at 877k cells.

    -- 1. Get project's active cost profile
    SELECT active_cost_profile_id
    INTO v_active_profile_id
    FROM bioprotect.projects
    WHERE id = p_project_id;

    -- 2. If no active profile, try default cost profile for project
    IF v_active_profile_id IS NULL THEN
        SELECT id
        INTO v_active_profile_id
        FROM bioprotect.cost_profiles
        WHERE project_id = p_project_id
          AND is_default = TRUE
        ORDER BY id
        LIMIT 1;
    END IF;

    -- 3. If still no profile, fallback: cost = 0, status = 0
    IF v_active_profile_id IS NULL THEN
        RETURN QUERY
        SELECT
            pp.h3_index,
            0::NUMERIC    AS cost,
            0::INTEGER    AS status
        FROM bioprotect.project_pus pp
        WHERE pp.project_id = p_project_id;
        RETURN;
    END IF;

    -- 4. Normal path: use active (or default) cost profile
    RETURN QUERY
    SELECT
        pp.h3_index,
        cpv.cost,
        cpv.status
    FROM bioprotect.cost_profile_values cpv
    JOIN bioprotect.project_pus pp
      ON pp.id = cpv.project_pu_id
    WHERE cpv.cost_profile_id = v_active_profile_id
      AND pp.project_id = p_project_id;

END;
$function$

