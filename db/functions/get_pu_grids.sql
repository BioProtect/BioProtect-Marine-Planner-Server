CREATE OR REPLACE FUNCTION bioprotect.get_pu_grids()
 RETURNS TABLE(alias text, description text, creation_date text, aoi_id integer, domain text, _area double precision, envelope text, source text, created_by text, tilesetid text, planning_unit_count integer)
 LANGUAGE plpgsql
AS $function$
BEGIN
  RETURN QUERY
  SELECT DISTINCT
      pu.alias,
      pu.description,
      to_char(pu.creation_date, 'DD/MM/YY HH24:MI:SS') AS creation_date,
      pu.aoi_id,
      pu.domain,
      pu._area,
      ST_AsText(pu.envelope),
      pu.source,
      pu.created_by,
      pu.tilesetid,
      pu.planning_unit_count
  FROM bioprotect.metadata_planning_units pu
  WHERE pu.tilesetid IS NOT NULL  -- optionally only return entries that are H3-enabled
  ORDER BY pu.alias;
END;
$function$

