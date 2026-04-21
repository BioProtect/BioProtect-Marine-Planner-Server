CREATE OR REPLACE FUNCTION bioprotect.get_project_species(p_project_id integer)
 RETURNS TABLE(feature_unique_id integer, target_type text, target_value numeric, spf numeric, weight numeric, created_at timestamp with time zone, updated_at timestamp with time zone, alias text, feature_class_name text, description text, area numeric, extent text, creation_date text, tilesetid text, created_by text)
 LANGUAGE sql
 STABLE
AS $function$
    SELECT
        pf.feature_unique_id,
        pf.target_type,
        pf.target_value,
        pf.spf,
        pf.weight,
        pf.created_at,
        pf.updated_at,

        f.alias,
        f.feature_class_name,
        f.description,
        f._area AS area,
        f.extent,
        TO_CHAR(f.creation_date, 'Dy, DD Mon YYYY HH24:MI:SS') AS creation_date,
        f.tilesetid,
        f.created_by

    FROM bioprotect.project_features pf
    JOIN bioprotect.metadata_interest_features f
      ON f.unique_id = pf.feature_unique_id
    WHERE pf.project_id = p_project_id
    ORDER BY f.alias;
$function$

