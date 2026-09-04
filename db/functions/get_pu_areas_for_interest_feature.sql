CREATE OR REPLACE FUNCTION bioprotect.get_pu_areas_for_interest_feature(pu_name text, interest_feature_name text)
 RETURNS TABLE(species integer, pu integer, amount double precision)
 LANGUAGE plpgsql
AS $function$ 
DECLARE
BEGIN
return query EXECUTE 'SELECT metadata.unique_id::integer species, puid pu, sum(ST_Area(ST_Intersection(grid.geometry,feature.geometry))) amount from bioprotect.' || $1 || ' grid, bioprotect.' || $2 || ' feature, bioprotect.metadata_interest_features metadata where st_intersects(grid.geometry,feature.geometry) and metadata.feature_class_name = ''' || $2 || ''' group by 1,2;';

END
$function$

