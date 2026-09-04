CREATE OR REPLACE FUNCTION bioprotect.updatefeaturesbatch()
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    row     record;
	unitCount int;
BEGIN
    FOR row IN 
        SELECT feature_class_name FROM bioprotect.metadata_interest_features 
    LOOP
		--populate the planning unit count field in the metadata_planning_units table
-- 		EXECUTE 'SELECT count(puid) FROM bioprotect.' || quote_ident(row.feature_class_name) || ';' INTO unitCount;
--         EXECUTE 'UPDATE bioprotect.metadata_planning_units SET planning_unit_count = ' || unitCount || ' WHERE feature_class_name = ''' || quote_ident(row.feature_class_name) || ''';';
--         RAISE INFO 'Updated metadata_planning_units for table: %', quote_ident(row.feature_class_name);
--         RAISE INFO 'Updated metadata_planning_units for table: %', quote_ident(row.feature_class_name);

		--update the geometries in all the planning unit feature classes to EPSG:4326
		EXECUTE 'ALTER TABLE bioprotect.' || quote_ident(row.feature_class_name) || ' ADD COLUMN geometry2 geometry;';
		EXECUTE 'UPDATE bioprotect.' || quote_ident(row.feature_class_name) || ' SET geometry2=ST_Transform(geometry,4326);';
		EXECUTE 'ALTER TABLE bioprotect.' || quote_ident(row.feature_class_name) || ' DROP COLUMN geometry;';
		EXECUTE 'ALTER TABLE bioprotect.' || quote_ident(row.feature_class_name) || ' RENAME COLUMN geometry2 TO geometry;';
-- 		EXECUTE 'SELECT UpdateGeometrySRID(''bioprotect'',''' || quote_ident(row.feature_class_name) || ''',''geometry'',3410);';
		EXECUTE 'CREATE INDEX idx_' || quote_ident(row.feature_class_name) || '_01 ON bioprotect.' || quote_ident(row.feature_class_name) || ' USING GIST (geometry);';
        RAISE INFO 'Updated geometry for table: %', quote_ident(row.feature_class_name);
    END LOOP;
END;
$function$

