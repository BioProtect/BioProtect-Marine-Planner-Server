CREATE OR REPLACE FUNCTION bioprotect.clear_feature_data(p_project_id integer, p_feature_id integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
BEGIN
    DELETE FROM bioprotect.pu_feature_amounts
    WHERE project_id = p_project_id
      AND feature_unique_id = p_feature_id;

    DELETE FROM bioprotect.feature_preprocessing
    WHERE project_id = p_project_id
      AND feature_unique_id = p_feature_id;
END;
$function$

