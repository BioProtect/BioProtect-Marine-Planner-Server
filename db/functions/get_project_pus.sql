CREATE OR REPLACE FUNCTION bioprotect.get_project_pus(project_id_input integer)
 RETURNS TABLE(id text, cost double precision, status integer)
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN QUERY
SELECT h3_index AS id, cost, status
FROM bioprotect.project_pus
WHERE project_id = project_id_input
AND h3_index IN (
  SELECT h3_index
  FROM bioprotect.h3_cells
  WHERE resolution = 6
);
END;
$function$

