CREATE OR REPLACE FUNCTION bioprotect.get_planning_units_metadata(planning_unit_id bigint)
 RETURNS TABLE(feature_class_name text, alias text, description text, creation_date text, domain text, area double precision, created_by text)
 LANGUAGE sql
AS $function$SELECT 
  feature_class_name,
  alias, 
  description, 
  creation_date::text, 
  domain, 
  _area area, 
  created_by 
FROM bioprotect.metadata_planning_units pu 
WHERE pu.unique_id = $1::integer;
$function$

