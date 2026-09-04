CREATE OR REPLACE FUNCTION bioprotect.gap_analysis(planning_grid_name text, feature_ids integer[], _user text, project text)
 RETURNS TABLE(_feature_class_name text, _alias text, total_area double precision, country_area double precision, current_protected_area double precision, current_protected_percent double precision, endemic boolean)
 LANGUAGE plpgsql
AS $function$

DECLARE
	_country_id integer;
	_domain text;
	country_geometry geometry;
	pa_union_geometry geometry;
	feature_geometries geometry[];
	feature_total_area double precision;
	feature_country_area double precision;
	feature_protected_area double precision;
	_iso3 text;
	fc record;
	query text;
	dissolved_wdpa_featureclass_exists boolean DEFAULT False;
	dissolved_wdpa_featureclass_name text;
	dissolved_wdpa_featureclass_index_name text;
	tmp_table text;
	tmp_table_exists text;
	feature_count integer;
	counter integer := 1;
BEGIN
--GET THE NAME OF THE TABLE THAT WILL BE PRODUCED
tmp_table := lower(format('gap_%s_%s', $3, $4));
--SEE IF IT ALREADY EXISTS
EXECUTE 'SELECT to_regclass(''bioprotect.' || quote_ident(tmp_table) || ''')' INTO tmp_table_exists;

--IF IT DOES NOT EXIST, THEN DO THE GAP ANALYSIS
IF tmp_table_exists IS NULL THEN 
	--1. GET THE PLANNING GRID GEOMETRY, i.e. country boundary
	SELECT domain, country_id FROM  bioprotect.metadata_planning_units WHERE feature_class_name = $1 INTO _domain, _country_id;
	SELECT geom, iso3 FROM bioprotect.gaul_eez_dissolved WHERE country_id = _country_id INTO country_geometry, _iso3;
	RAISE DEBUG 'Country geometry: % (%)', substring(ST_AsText(country_geometry) FROM 0 for 50), clock_timestamp();

	--2. GET THE DISSOLVED PROTECTED AREAS FOR THE COUNTRY
	dissolved_wdpa_featureclass_name := format('wdpa_%s_dissolved',_country_id);
	--see if the dissolved protected areas feature class already exists for this country
	EXECUTE 'SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE  table_schema = ''bioprotect'' AND table_name = ''' || dissolved_wdpa_featureclass_name || ''');' INTO dissolved_wdpa_featureclass_exists;
	IF NOT dissolved_wdpa_featureclass_exists THEN
		--if the dissolved protected areas feature class does not already exist, then dissolve it and store the results
		RAISE DEBUG 'Dissolving protected areas in: % (%)', _iso3, clock_timestamp();
		EXECUTE 'CREATE TABLE bioprotect.'|| dissolved_wdpa_featureclass_name || ' AS SELECT ST_MakeValid(ST_Union(ST_MakeValid(geometry))) geom FROM bioprotect.wdpa WHERE iso3 = '''|| _iso3 || ''' AND status NOT IN (''Proposed'',''Not Reported'') AND desig NOT IN(''World Heritage Site (natural or mixed)'',''Biosphere Reserve'');';
		--create a spatial index to help with the intersection
		dissolved_wdpa_featureclass_index_name := dissolved_wdpa_featureclass_name || '_idx';
		EXECUTE 'CREATE INDEX ' || dissolved_wdpa_featureclass_index_name || ' ON bioprotect.' || dissolved_wdpa_featureclass_name || ' USING GIST (geom);';
	ELSE
		RAISE DEBUG 'Using existing dissolved protected areas featureclass: %', dissolved_wdpa_featureclass_name;
	END IF;
	EXECUTE 'SELECT * FROM marxan.'|| dissolved_wdpa_featureclass_name INTO pa_union_geometry;
	RAISE DEBUG 'Dissolved protected area geometry: % (%)', substring(ST_AsText(pa_union_geometry) FROM 0 for 50), clock_timestamp();

	--3. GET THE INTERSECTIONS FOR THE FEATURES
	RAISE DEBUG 'Creating the results in % table', tmp_table;
	EXECUTE 'DROP TABLE IF EXISTS marxan.' || tmp_table || ';';
	EXECUTE 'CREATE TABLE marxan.' || tmp_table || '(    feature_class_name text COLLATE pg_catalog."default",	alias text COLLATE pg_catalog."default",    total_area double precision,	country_area double precision,    current_protected_area double precision,    current_protected_percent double precision);';
	feature_count := array_length(feature_ids,1);
	FOR fc IN SELECT oid, feature_class_name, alias FROM bioprotect.metadata_interest_features WHERE oid = ANY (feature_ids) LOOP
		RAISE DEBUG '---------------------------------------------------------';
		RAISE DEBUG ' % (%) (%/%)',  fc.alias,fc.oid, counter, feature_count;
		--3a DISSOLVE THE FEATURE IF IT IS FROM GBIF AS THESE RECORDS WILL OVERLAP
		IF substring(fc.feature_class_name from 1 for 5) = 'gbif_' THEN
			RAISE DEBUG ' Dissolving features';	
			EXECUTE 'SELECT array(SELECT (ST_Dump(ST_Union(geometry))).geom FROM bioprotect.' || quote_ident(fc.feature_class_name) || ')' INTO feature_geometries;
		ELSE
			EXECUTE 'SELECT array(SELECT geometry FROM bioprotect.' || quote_ident(fc.feature_class_name) || ')' INTO feature_geometries;
		END IF;
		RAISE DEBUG ' % features', array_length(feature_geometries,1);	
		--3b GET THE FEATURE AREA
		EXECUTE 'SELECT SUM(ST_Area(ST_Transform(geom, 3410))) FROM (SELECT unnest($1) geom) AS features' INTO feature_total_area USING feature_geometries;
		RAISE DEBUG ' Total area: %m2', feature_total_area;
		--3c GET THE FEATURE AREA FOR THE COUNTRY
		EXECUTE 'SELECT SUM(ST_Area(ST_Transform(ST_Intersection(geom, $1),3410))) FROM (SELECT unnest($2) geom) AS features' INTO feature_country_area USING country_geometry, feature_geometries;
		RAISE DEBUG ' Area within country: %m2', feature_country_area;
		--3d GET THE FEATURE AREA THAT IS WITHIN THE PROTECTED AREAS
		EXECUTE 'SELECT COALESCE(SUM(ST_Area(ST_Transform(ST_Intersection(features.geom, pas.geom),3410))),0) FROM (SELECT unnest($2) geom) AS features, (SELECT (ST_Dump($1)).geom) AS pas WHERE ST_Intersects(features.geom, pas.geom)' INTO feature_protected_area USING pa_union_geometry, feature_geometries; 
		RAISE DEBUG ' Area within protected areas: %m2', feature_protected_area;
		--3e INSERT THE DATA INTO THE _tmp TABLE
		EXECUTE 'INSERT INTO bioprotect.' || tmp_table || ' VALUES('''|| fc.feature_class_name || ''','''|| fc.alias || ''',' || feature_total_area || ',' || feature_country_area || ',' || feature_protected_area || ',' || (feature_protected_area/feature_country_area)*100 || ');';
		RAISE DEBUG ' %', clock_timestamp();
		counter := counter + 1;
	END LOOP;
END IF;

RETURN QUERY EXECUTE 'SELECT *, (' || tmp_table || '.country_area = ' || tmp_table || '.total_area)  FROM bioprotect.' || tmp_table || ' ORDER BY alias;';

RETURN;

EXCEPTION
    WHEN OTHERS THEN
		RAISE NOTICE 'An error occurred: %', SQLSTATE;
		RAISE NOTICE 'Error message: %', SQLERRM;
END

$function$

