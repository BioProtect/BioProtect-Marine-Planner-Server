CREATE OR REPLACE FUNCTION bioprotect.set_active_profile_pu_statuses(
    p_project_id INT,
    p_status1_h3 TEXT[] DEFAULT ARRAY[]::TEXT[],
    p_status2_h3 TEXT[] DEFAULT ARRAY[]::TEXT[],
    p_status3_h3 TEXT[] DEFAULT ARRAY[]::TEXT[]
)
RETURNS VOID
LANGUAGE plpgsql AS
$$
DECLARE
    v_profile_id INT;
BEGIN
    -- Active profile must exist if status lives in profiles
    SELECT active_cost_profile_id
    INTO v_profile_id
    FROM bioprotect.projects
    WHERE id = p_project_id;

    IF v_profile_id IS NULL THEN
        RAISE EXCEPTION 'Project % has no active_cost_profile_id', p_project_id;
    END IF;

    -- Single-pass update using CASE
    UPDATE bioprotect.cost_profile_values cpv
    SET status =
        CASE
            WHEN pp.h3_index = ANY(p_status1_h3) THEN 1
            WHEN pp.h3_index = ANY(p_status2_h3) THEN 2
            WHEN pp.h3_index = ANY(p_status3_h3) THEN 3
            ELSE 0
        END
    FROM bioprotect.project_pus pp
    WHERE cpv.cost_profile_id = v_profile_id
      AND cpv.project_pu_id = pp.id
      AND pp.project_id = p_project_id;

END;
$$;
