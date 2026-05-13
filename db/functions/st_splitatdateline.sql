CREATE OR REPLACE FUNCTION bioprotect.st_splitatdateline(feature geometry)
 RETURNS geometry
 LANGUAGE plpgsql
AS $function$
DECLARE
 minx double precision;
BEGIN

IF ST_Intersects(ST_GeogFromText('SRID=4326;LINESTRING(180 -90, 180 0,180 90)'), feature) THEN
	RAISE DEBUG 'Splitting feature on the dateline with first point: %', ST_AsText(ST_StartPoint(ST_Boundary(feature)));
END IF;

RETURN CASE 
      WHEN ST_Intersects(ST_GeogFromText('SRID=4326;LINESTRING(180 -90, 180 0,180 90)'), feature) THEN 
	  	CASE
			WHEN ST_XMin(feature) > 0 THEN
			  	ST_CollectionExtract(ST_WrapX(ST_Split(feature, ST_ShiftLongitude(ST_GeogFromText('SRID=4326;LINESTRING(180 -90, 180 0,180 90)')::geometry)), 180, -360),3)
			ELSE
				ST_CollectionExtract(ST_WrapX(ST_Split(ST_ShiftLongitude(feature), ST_ShiftLongitude(ST_GeogFromText('SRID=4326;LINESTRING(180 -90, 180 0,180 90)')::geometry)), 180, -360),3)
			END
      ELSE feature
END ;

END;
$function$

