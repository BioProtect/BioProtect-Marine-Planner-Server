CREATE OR REPLACE FUNCTION bioprotect.aggregate_feature_stats(p_project_id integer, p_feature_id integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    INSERT INTO bioprotect.feature_preprocessing (project_id, feature_unique_id, pu_area, pu_count, updated_at)
    SELECT
        p_project_id,
        p_feature_id,
        SUM(amount),
        COUNT(*),
        now()
    FROM bioprotect.pu_feature_amounts
    WHERE project_id = p_project_id
      AND feature_unique_id = p_feature_id
    GROUP BY project_id, feature_unique_id
    ON CONFLICT (project_id, feature_unique_id)
    DO UPDATE SET
        pu_area   = EXCLUDED.pu_area,
        pu_count  = EXCLUDED.pu_count,
        updated_at = now();
END;
$function$

