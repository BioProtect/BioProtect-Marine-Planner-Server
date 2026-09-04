CREATE OR REPLACE FUNCTION bioprotect.get_project_features(p_project_id integer)
 RETURNS TABLE(unique_id integer, feature_class_name text, alias text, description text, creation_date timestamp without time zone, area double precision, tilesetid text, extent box2d, source text, created_by text, target_type text, target_value numeric, spf numeric, weight numeric, created_at timestamp with time zone, updated_at timestamp with time zone)
 LANGUAGE plpgsql
 STABLE
AS $function$
BEGIN
    RETURN QUERY
    SELECT
        f.unique_id,
        f.feature_class_name,
        f.alias,
        f.description,
        f.creation_date,
        f._area AS area,
        f.tilesetid,
        f.extent,
        f.source,
        f.created_by,
        pf.target_type,
        pf.target_value,
        pf.spf,
        pf.weight,
        pf.created_at,
        pf.updated_at
    FROM bioprotect.project_features pf
    JOIN bioprotect.metadata_interest_features f
      ON f.unique_id = pf.feature_unique_id
    WHERE pf.project_id = p_project_id
    ORDER BY f.alias;
END;
$function$

