CREATE OR REPLACE FUNCTION bioprotect.update_project_feature(p_project_id integer, p_feature_id integer, p_target_type text, p_target_value numeric, p_spf numeric, p_weight numeric)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    -- NULL target = not set yet (a feature just added to the project). Stored
    -- as NULL so get_prioritizr_run_config falls back to its 30% default; on
    -- update the COALESCE below keeps any target already set.
    -- 3) Upsert into project_features
    INSERT INTO bioprotect.project_features (
        project_id, feature_unique_id, target_type, target_value, spf, weight
    )
    VALUES (p_project_id, p_feature_id, p_target_type, p_target_value, p_spf, p_weight)
    ON CONFLICT (project_id, feature_unique_id)
    DO UPDATE SET
        target_type  = COALESCE(EXCLUDED.target_type,  project_features.target_type),
        target_value = COALESCE(EXCLUDED.target_value, project_features.target_value),
        spf          = COALESCE(EXCLUDED.spf,          project_features.spf),
        weight       = COALESCE(EXCLUDED.weight,       project_features.weight),
        updated_at   = now();
END;
$function$

