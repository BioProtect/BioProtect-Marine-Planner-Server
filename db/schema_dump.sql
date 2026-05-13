--
-- PostgreSQL database dump
--

\restrict 0b23OcroK2Hxr739hx8egrT1DZzdLdWmLFARWDryvx9Sl74XzCLHVdGLdfaSPZp

-- Dumped from database version 15.17 (Ubuntu 15.17-1.pgdg24.04+1)
-- Dumped by pg_dump version 18.3 (Ubuntu 18.3-1.pgdg24.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: bioprotect; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA bioprotect;


--
-- Name: SCHEMA bioprotect; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON SCHEMA bioprotect IS 'Schema for the Marxan Systematic Conservation Planning software';


--
-- Name: bbox; Type: TYPE; Schema: bioprotect; Owner: -
--

CREATE TYPE bioprotect.bbox AS (
	countryid integer,
	countryname text,
	minx double precision,
	miny double precision,
	maxx double precision,
	maxy double precision
);


--
-- Name: aggregate_feature_stats(integer, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.aggregate_feature_stats(p_project_id integer, p_feature_id integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    INSERT INTO bioprotect.feature_preprocessing (project_id, feature_unique_id, pu_area, pu_count, updated_at)
    SELECT
        p_project_id,
        p_feature_id,
        SUM(amount),
        COUNT(*),
        now()
    FROM bioprotect.pu_feature_amounts
    WHERE project_id = p_project_id
      AND feature_unique_id = p_feature_id
    GROUP BY project_id, feature_unique_id
    ON CONFLICT (project_id, feature_unique_id)
    DO UPDATE SET
        pu_area   = EXCLUDED.pu_area,
        pu_count  = EXCLUDED.pu_count,
        updated_at = now();
END;
$$;


--
-- Name: clear_feature_data(integer, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.clear_feature_data(p_project_id integer, p_feature_id integer) RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    DELETE FROM bioprotect.pu_feature_amounts
    WHERE project_id = p_project_id
      AND feature_unique_id = p_feature_id;

    DELETE FROM bioprotect.feature_preprocessing
    WHERE project_id = p_project_id
      AND feature_unique_id = p_feature_id;
END;
$$;


--
-- Name: create_pressures_from_activity(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.create_pressures_from_activity(_activity_id integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    _activity_title TEXT;
    _activity_table TEXT;
    _activity_geom  geometry;
    _count          INTEGER;
BEGIN
    -- Get activity metadata
    SELECT activity, activity_name
      INTO _activity_title, _activity_table
      FROM bioprotect.metadata_activities
     WHERE id = _activity_id;

    IF _activity_title IS NULL THEN
        RAISE EXCEPTION 'Activity % not found', _activity_id;
    END IF;

    IF _activity_table IS NULL THEN
        RAISE EXCEPTION 'Activity % has no geometry table', _activity_id;
    END IF;

    -- Union all geometries from the activity table into one
    EXECUTE format(
        'SELECT ST_Union(geometry) FROM bioprotect.%I',
        _activity_table
    ) INTO _activity_geom;

    IF _activity_geom IS NULL THEN
        RAISE EXCEPTION 'Activity table % has no geometry data', _activity_table;
    END IF;

    -- Delete any existing pressures for this activity (re-runnable)
    DELETE FROM bioprotect.pressures WHERE activity_id = _activity_id;

    -- Insert one pressure per PAD entry for this activity
    INSERT INTO bioprotect.pressures
        (activity_id, pressuretitle, rppscore, geometry)
    SELECT _activity_id,
           pad.pressuretitle,
           pad.rppscore,
           _activity_geom
      FROM bioprotect.pad pad
     WHERE pad.activitytitle = _activity_title
       AND pad.rppscore > 0;

    GET DIAGNOSTICS _count = ROW_COUNT;
    RETURN _count;
END;
$$;


--
-- Name: deletedissolvedwdpafeatureclasses(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.deletedissolvedwdpafeatureclasses() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    row     record;
BEGIN
    FOR row IN 
        SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'bioprotect' AND table_name ILIKE ('wdpa_%') ORDER BY 1
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$$;


--
-- Name: FUNCTION deletedissolvedwdpafeatureclasses(); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.deletedissolvedwdpafeatureclasses() IS 'Iterates through all of the dissolved country WDPA feature classes and deletes them, e.g. if a new version of the WDPA has been installed';


--
-- Name: deleteorphanedfeatures(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.deleteorphanedfeatures() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    row     record;
BEGIN
    FOR row IN 
        SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'bioprotect' AND table_name ILIKE ('f_%') AND table_name NOT IN (SELECT feature_class_name FROM bioprotect.metadata_interest_features) ORDER BY 1
    LOOP
        EXECUTE 'DROP TABLE bioprotect.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
    FOR row IN 
        SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'bioprotect' AND table_name ILIKE ('pu_%') AND table_name NOT IN (SELECT feature_class_name FROM bioprotect.metadata_planning_units) ORDER BY 1
    LOOP
        EXECUTE 'DROP TABLE bioprotect.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$$;


--
-- Name: FUNCTION deleteorphanedfeatures(); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.deleteorphanedfeatures() IS 'Server function to remove all the feature classes that have been orphaned in the metadata_interest_feature table and metadata_planning_units tables';


--
-- Name: deletescratchfeatureclasses(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.deletescratchfeatureclasses() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    row     record;
BEGIN
    FOR row IN 
        SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema = 'bioprotect' AND table_name ILIKE ('scratch_%') ORDER BY 1
    LOOP
        EXECUTE 'DROP TABLE ' || quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
        RAISE INFO 'Dropped table: %', quote_ident(row.table_schema) || '.' || quote_ident(row.table_name);
    END LOOP;
END;
$$;


--
-- Name: FUNCTION deletescratchfeatureclasses(); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.deletescratchfeatureclasses() IS 'Deletes all of the scratch_* feature classes that may be left in the db';


--
-- Name: gap_analysis(text, integer[], text, text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.gap_analysis(planning_grid_name text, feature_ids integer[], _user text, project text) RETURNS TABLE(_feature_class_name text, _alias text, total_area double precision, country_area double precision, current_protected_area double precision, current_protected_percent double precision, endemic boolean)
    LANGUAGE plpgsql
    AS $_$

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

$_$;


--
-- Name: get_planning_units_for_project(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_planning_units_for_project(p_project_id integer) RETURNS TABLE(h3_index text, cost numeric, status integer)
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
    v_active_profile_id INT;
    v_domain            TEXT;
    v_resolution        INT;
BEGIN
    -- 1. Get project's resolution + active cost profile
    SELECT default_resolution, active_cost_profile_id
    INTO v_resolution, v_active_profile_id
    FROM bioprotect.projects
    WHERE id = p_project_id;

    -- 2. Get domain/project area label for matching against h3_cells
    SELECT LOWER(TRIM(split_part(mpu.alias, ' (', 1)))
    INTO v_domain
    FROM bioprotect.projects p
    JOIN bioprotect.metadata_planning_units mpu
      ON p.planning_unit_id = mpu.unique_id
    WHERE p.id = p_project_id;

    -- 3. If no active profile, try default cost profile for project
    IF v_active_profile_id IS NULL THEN
        SELECT id
        INTO v_active_profile_id
        FROM bioprotect.cost_profiles
        WHERE project_id = p_project_id
          AND is_default = TRUE
        ORDER BY id
        LIMIT 1;
    END IF;

    -- 4. If still no profile, fallback: cost = 0, status = 0
    IF v_active_profile_id IS NULL THEN
        RETURN QUERY
        SELECT
            pp.h3_index,
            0::NUMERIC    AS cost,
            0::INTEGER    AS status
        FROM bioprotect.project_pus pp
        JOIN bioprotect.h3_cells hc
          ON hc.h3_index = pp.h3_index
        WHERE pp.project_id = p_project_id
          AND LOWER(TRIM(hc.project_area)) = v_domain;
        RETURN;
    END IF;

    -- 5. Normal path: use active (or default) cost profile
    RETURN QUERY
    SELECT
        pp.h3_index,
        cpv.cost,
        cpv.status
    FROM bioprotect.cost_profile_values cpv
    JOIN bioprotect.project_pus pp
      ON pp.id = cpv.project_pu_id
    JOIN bioprotect.h3_cells hc
      ON hc.h3_index = pp.h3_index
    WHERE cpv.cost_profile_id = v_active_profile_id
      AND pp.project_id = p_project_id
      AND LOWER(TRIM(hc.project_area)) = v_domain;

END;
$$;


--
-- Name: get_planning_units_metadata(bigint); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_planning_units_metadata(planning_unit_id bigint) RETURNS TABLE(feature_class_name text, alias text, description text, creation_date text, domain text, country text, area double precision, created_by text)
    LANGUAGE sql
    AS $_$SELECT 
  feature_class_name,
  alias, 
  description, 
  creation_date::text, 
  domain, 
  original_n AS country, 
  _area area, 
  created_by 
FROM bioprotect.metadata_planning_units pu 
LEFT OUTER JOIN bioprotect.gaul_2015_simplified_1km 
ON id_country = country_id 
WHERE pu.unique_id = $1::integer;
$_$;


--
-- Name: get_prioritizr_run_config(bigint); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_prioritizr_run_config(p_run_id bigint) RETURNS TABLE(run_id bigint, project_id integer, input_table text, feature_cols text[], target_prop double precision, mode text, boundary_penalty double precision, linear_cost_penalty double precision, gap double precision, time_limit_sec integer, feature_targets_json text)
    LANGUAGE sql STABLE
    AS $$
SELECT
  r.id AS run_id,
  r.project_id,
  r.input_table,
  r.feature_cols,
  COALESCE((r.params->'targets'->>'prop')::double precision, 0.30) AS target_prop,
  COALESCE((r.params->>'mode')::text, 'area') AS mode,
  COALESCE((r.params->'penalties'->>'boundary')::double precision, 0.0) AS boundary_penalty,
  COALESCE((r.params->'penalties'->>'linear')::double precision, 0.1) AS linear_cost_penalty,
  COALESCE((r.params->'solver'->>'gap')::double precision, 0.10) AS gap,
  COALESCE((r.params->'solver'->>'time_limit')::int, 1200) AS time_limit_sec,
  -- Build {"f_19": 0.3, "f_21": 0.5, ...} from project_features.target_value (0-100 → 0-1)
  (SELECT jsonb_object_agg(
            format('f_%s', pf.feature_unique_id),
            COALESCE(pf.target_value, 30) / 100.0
          )::text
   FROM bioprotect.project_features pf
   WHERE pf.project_id = r.project_id
  ) AS feature_targets_json
FROM bioprotect.prioritizr_runs r
WHERE r.id = p_run_id;
$$;


--
-- Name: get_project_boundary_edges(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_project_boundary_edges(p_project_id integer) RETURNS TABLE(pu_id text, nbr_id text, boundary double precision)
    LANGUAGE sql STABLE
    AS $$
  SELECT gbe.h3_a AS pu_id,
         gbe.h3_b AS nbr_id,
         gbe.boundary
  FROM bioprotect.grid_boundary_edges gbe
  JOIN bioprotect.projects pr ON pr.planning_unit_id = gbe.planning_unit_id
  WHERE pr.id = p_project_id;
$$;


--
-- Name: get_project_features(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_project_features(p_project_id integer) RETURNS TABLE(unique_id integer, feature_class_name text, alias text, description text, creation_date timestamp without time zone, area double precision, tilesetid text, extent public.box2d, source text, created_by text, target_type text, target_value numeric, spf numeric, weight numeric, created_at timestamp with time zone, updated_at timestamp with time zone)
    LANGUAGE plpgsql STABLE
    AS $$
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
$$;


--
-- Name: get_project_h3_adjacency(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_project_h3_adjacency(p_project_id integer) RETURNS TABLE(pu_id text, nbr_id text, boundary double precision)
    LANGUAGE sql STABLE
    AS $$
WITH pus AS (
  SELECT pp.h3_index::text AS pu_id
  FROM bioprotect.project_pus pp
  WHERE pp.project_id = p_project_id
),
edges AS (
  SELECT
    p.pu_id,
    n::text AS nbr_id
  FROM pus p
  JOIN LATERAL h3_grid_disk(p.pu_id::h3index, 1) AS n ON TRUE
  WHERE n::text <> p.pu_id
)
SELECT
  LEAST(e.pu_id, e.nbr_id)     AS pu_id,
  GREATEST(e.pu_id, e.nbr_id)  AS nbr_id,
  1.0                          AS boundary
FROM edges e
JOIN pus p2
  ON p2.pu_id = e.nbr_id
GROUP BY 1,2,3;
$$;


--
-- Name: get_project_pus(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_project_pus(project_id_input integer) RETURNS TABLE(id text, cost double precision, status integer)
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: FUNCTION get_project_pus(project_id_input integer); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.get_project_pus(project_id_input integer) IS 'code to create data similar to what used to be used in pu.dat';


--
-- Name: get_project_species(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_project_species(p_project_id integer) RETURNS TABLE(feature_unique_id integer, target_type text, target_value numeric, spf numeric, weight numeric, created_at timestamp with time zone, updated_at timestamp with time zone, alias text, feature_class_name text, description text, area numeric, extent text, creation_date text, tilesetid text, created_by text)
    LANGUAGE sql STABLE
    AS $$
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
$$;


--
-- Name: get_projects_for_user(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_projects_for_user(p_user_id integer) RETURNS TABLE(id integer, name text, description text, date_created timestamp without time zone, planning_unit_id integer, old_version boolean, iucn_category text, is_private boolean, costs text, default_resolution integer, planning_unit_alias text, role text)
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
    RETURN QUERY
    SELECT 
        p.id,
        p.name,
        p.description,
        p.date_created,
        p.planning_unit_id,
        p.old_version,
        p.iucn_category,
        p.is_private,
        p.costs,
        p.default_resolution,
        pu.alias AS planning_unit_alias,
        up.role
    FROM bioprotect.projects p
    JOIN bioprotect.user_projects up
      ON up.project_id = p.id
    LEFT JOIN bioprotect.metadata_planning_units pu
      ON p.planning_unit_id = pu.unique_id
    WHERE up.user_id = p_user_id
    ORDER BY LOWER(p.name);
END;
$$;


--
-- Name: get_pu_areas_for_interest_feature(text, text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_pu_areas_for_interest_feature(pu_name text, interest_feature_name text) RETURNS TABLE(species integer, pu integer, amount double precision)
    LANGUAGE plpgsql
    AS $_$ 
DECLARE
BEGIN
return query EXECUTE 'SELECT metadata.unique_id::integer species, puid pu, sum(ST_Area(ST_Intersection(grid.geometry,feature.geometry))) amount from bioprotect.' || $1 || ' grid, bioprotect.' || $2 || ' feature, bioprotect.metadata_interest_features metadata where st_intersects(grid.geometry,feature.geometry) and metadata.feature_class_name = ''' || $2 || ''' group by 1,2;';

END
$_$;


--
-- Name: get_pu_grids(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_pu_grids() RETURNS TABLE(alias text, description text, creation_date text, country_id integer, aoi_id integer, domain text, _area double precision, envelope text, source text, country text, created_by text, tilesetid text, planning_unit_count integer)
    LANGUAGE plpgsql
    AS $$
BEGIN
  RETURN QUERY
  SELECT DISTINCT
      pu.alias,
      pu.description,
      to_char(pu.creation_date, 'DD/MM/YY HH24:MI:SS') AS creation_date,
      pu.country_id,
      pu.aoi_id,
      pu.domain,
      pu._area,
      ST_AsText(pu.envelope),
      pu.source,
      g.original_n::text AS country,
      pu.created_by,
      pu.tilesetid,
      pu.planning_unit_count
  FROM bioprotect.metadata_planning_units pu
  LEFT JOIN bioprotect.gaul_2015_simplified_1km g
    ON g.id_country = pu.country_id
  WHERE pu.tilesetid IS NOT NULL  -- optionally only return entries that are H3-enabled
  ORDER BY pu.alias;
END;
$$;


--
-- Name: insert_feature_pu_amounts(integer, integer, integer, text, text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.insert_feature_pu_amounts(p_project_id integer, p_feature_id integer, p_planning_unit_id integer, p_feature_class text, p_geom_type text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_grid_table TEXT;
BEGIN
    -- Resolve grid table name from metadata_planning_units
    SELECT feature_class_name
      INTO v_grid_table
      FROM bioprotect.metadata_planning_units
     WHERE unique_id = p_planning_unit_id;

    IF v_grid_table IS NULL THEN
        RAISE EXCEPTION 'No planning unit table found for ID %', p_planning_unit_id;
    END IF;

    RAISE NOTICE 'Using grid table: %, feature table: %', v_grid_table, p_feature_class;

    IF p_geom_type = 'ST_Point' THEN
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts
                (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index, SUM(feat.value)
            FROM bioprotect.%I AS grid
            JOIN bioprotect.%I AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_grid_table, p_feature_class);  -- ✅ correct order

    ELSE
        -- Polygon features: store coverage in km² (ST_Area gives m² in
        -- EPSG:3410, divide by 1e6). km² is the app-wide canonical unit
        -- for feature amounts — keeps numbers readable and avoids
        -- prioritizr's 1e6 presolve numerical cap.
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index,
                ST_Area(
                    ST_Transform(
                        ST_Union(ST_Intersection(grid.geometry, feat.geometry)),
                        3410
                    )
                ) / 1000000.0 AS amount
            FROM bioprotect.%I AS grid
            JOIN bioprotect.%I AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_grid_table, p_feature_class);
    END IF;
END;
$_$;


--
-- Name: planning_grid(double precision, text, text, text, text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.planning_grid(areakm2 double precision, iso3 text, domain text, shape text, _user text) RETURNS text
    LANGUAGE plpgsql
    AS $_$
DECLARE
    bounds bioprotect.bbox;
	gridTableName text DEFAULT lower(format('pu_%s_%s_%s_%s_grid',$2,$3,$4,$1));
	gridIndexName text DEFAULT lower(format('pu_%s_%s_%s_%s_grid_idx',$2,$3,$4,$1));
    tableName text DEFAULT lower(format('pu_%s_%s_%s_%s',$2,$3,$4,$1));
	unitCount int;
    aliasname text;
	query text;
BEGIN
  --create the table that will hold the grids that will be intersected with the area of interest to create the planning units for the marxan analysis
  EXECUTE 'DROP TABLE IF EXISTS bioprotect.' || gridTableName || ';';
  EXECUTE 'CREATE TABLE bioprotect.' || gridTableName || ' (id SERIAL, geometry geometry);';

  --get the min/max values for the country
  CASE 
  	WHEN ($2='FJI') THEN SELECT 95,'Fiji', 172.764,-25.09555598,-176.267,-9.783335456 INTO bounds;
	WHEN ($2='KIR') THEN SELECT 159,'Kiribati', 167.869,-13.83833333,-146.819,7.877488878 INTO bounds;
  	WHEN ($2='NZL') THEN SELECT 212,'New Zealand', 160.61,-55.9476816,-171.201,-25.88649522 INTO bounds;
	WHEN ($2='RUS') THEN SELECT 234,'Russian Federation', 18.926,39.81812505,-168.018,85.17640513 INTO bounds;
  	WHEN ($2='TUV') THEN SELECT 277,'Tuvalu', 172.711,-13.24038611,-176.756,-3.965563889 INTO bounds;
	WHEN ($2='USA') THEN SELECT 285,'United States of America', 167.641,15.56495332,-65.7,74.70758516 INTO bounds;
	WHEN ($2='WLF') THEN SELECT 298,'Wallis and Futuna', 179.505,-15.91930646,-174.267,-9.829646286 INTO bounds;
  ELSE
	  IF upper($3) = 'TERRESTRIAL' THEN
		SELECT id_country,name_iso31, ST_XMin(geom),ST_YMin(geom),ST_XMax(geom),ST_YMax(geom) INTO bounds FROM (SELECT id_country,name_iso31, ST_Envelope(wkb_geometry) geom FROM bioprotect.gaul_2015_simplified_1km g WHERE g.iso3 = $2) AS sub;
	  ELSE
		SELECT id_country,name_iso31, ST_XMin(geom),ST_YMin(geom),ST_XMax(geom),ST_YMax(geom) INTO bounds FROM (SELECT id_country,name_iso31, ST_Envelope(wkb_geometry) geom FROM bioprotect.eez_simplified_1km g WHERE g.iso3 = $2) AS sub;
	  END IF;
  END CASE;
  
  RAISE DEBUG 'bounds: (%)', bounds;
  IF bounds IS NULL THEN
      RAISE EXCEPTION 'The iso3 code does not exist';
  END IF;
  
  --get the alias name for the hexagons
  aliasname := format('%s %s %sKm2 %s grid', bounds.countryName,$3,$1,$4);
  
  --write the grids into the grid table
  IF upper($4) = 'HEXAGON' THEN
  	EXECUTE 'INSERT INTO bioprotect.' || gridTableName || '(geometry) SELECT bioprotect.ST_SplitAtDateline(ST_Transform(ST_SetSRID(bioprotect.hex_grid(' || areakm2 || ', ' || bounds.minx || ', ' || bounds.miny || ', ' || bounds.maxx || ', ' || bounds.maxy || '),3410),4326));';
  ELSE -- square
  	EXECUTE 'INSERT INTO bioprotect.' || gridTableName || '(geometry) SELECT bioprotect.ST_SplitAtDateline(ST_Transform(ST_SetSRID(bioprotect.square_grid(' || areakm2 || ', ' || bounds.minx || ', ' || bounds.miny || ', ' || bounds.maxx || ', ' || bounds.maxy || '),3410),4326));';
  END IF;
  
  --add a spatial index
  EXECUTE 'CREATE INDEX ' || gridIndexName || ' ON bioprotect.' || gridTableName || ' USING GIST (geometry);';
  
  --create the output table
  EXECUTE 'DROP TABLE IF EXISTS bioprotect.' || tableName || ';';
  EXECUTE 'CREATE TABLE bioprotect.' || tableName || ' (puid INTEGER, geometry geometry);';
  
  -- intersect the grid with the country boundary and write the results to the output table
  IF upper($3) = 'TERRESTRIAL' THEN
    EXECUTE 'INSERT INTO bioprotect.' || tableName || '(puid, geometry) SELECT m.id, m.geometry FROM bioprotect.' || gridTableName || ' m, bioprotect.gaul_2015_simplified_1km g WHERE ST_Intersects(ST_Transform(m.geometry, 4326), g.wkb_geometry) AND g.iso3 = ''' || $2 || ''';'; 
  ELSE
    EXECUTE 'INSERT INTO bioprotect.' || tableName || '(puid, geometry) SELECT m.id, m.geometry FROM bioprotect.' || gridTableName || ' m, bioprotect.eez_simplified_1km g WHERE ST_Intersects(ST_Transform(m.geometry, 4326), g.wkb_geometry) AND g.iso3 = ''' || $2 || ''';'; 
  END IF;

  --drop the temporary grid table
  EXECUTE 'DROP TABLE IF EXISTS bioprotect.' || gridTableName || ';';
  
  --add a spatial index on the output table
  EXECUTE 'CREATE INDEX ' || tableName || '_gix ON bioprotect.' || tableName || ' USING GIST (geometry);';

  --get the count of the planning grid units
  EXECUTE 'SELECT count(puid) FROM bioprotect.' || tableName || ';' INTO unitCount;
  RAISE INFO 'Count: %', unitCount;
  
  -- insert a record in the metadata table
  EXECUTE 'INSERT INTO bioprotect.metadata_planning_units(feature_class_name, alias, description, creation_date, country_id, aoi_id, domain, _area, envelope, source, created_by, tilesetid, planning_unit_count) values (''' || tableName || ''',''' || aliasname || ''',''Planning grid created with the planning_grid function'', now(), ' || bounds.countryId || ', null,''' || $3 || ''',' || $1 || ',marxan.ST_SplitAtDateline(ST_SetSRID(ST_Envelope(''POLYGON((' || bounds.minx || ' ' || bounds.miny || ',' || bounds.minx || ' ' || bounds.maxy || ',' || bounds.maxx || ' ' || bounds.maxy || ',' || bounds.maxx || ' ' || bounds.miny || ',' || bounds.minx || ' ' || bounds.miny || '))''::geometry),4326)), ''planning_grid function'',''' || $5 || ''',''blishten.' || tableName || ''',' || unitCount || ');';
  
  --return the alias of the feature class created
  RETURN aliasname;
  
  END

$_$;


--
-- Name: FUNCTION planning_grid(areakm2 double precision, iso3 text, domain text, shape text, _user text); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.planning_grid(areakm2 double precision, iso3 text, domain text, shape text, _user text) IS 'Creates a new planning grid. Domain can be terrestrial/marine and shape can be hexagon/square.';


--
-- Name: populate_grid_boundary_edges(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.populate_grid_boundary_edges(p_planning_unit_id integer) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_count BIGINT;
BEGIN
  -- Clear any existing edges for this grid
  DELETE FROM bioprotect.grid_boundary_edges
  WHERE planning_unit_id = p_planning_unit_id;

  -- Materialize the grid's h3 set, then compute neighbors via hash join.
  -- Much faster than correlated EXISTS for large grids (100K+ PUs).
  INSERT INTO bioprotect.grid_boundary_edges (planning_unit_id, h3_a, h3_b, boundary)
  SELECT
    p_planning_unit_id,
    LEAST(a.h3_index, b.h3_index),
    GREATEST(a.h3_index, b.h3_index),
    1.0
  FROM (
    SELECT DISTINCT pp.h3_index
    FROM bioprotect.project_pus pp
    JOIN bioprotect.projects pr ON pr.id = pp.project_id
    WHERE pr.planning_unit_id = p_planning_unit_id
  ) a
  JOIN LATERAL h3_grid_disk(a.h3_index::h3index, 1) AS n ON TRUE
  JOIN (
    SELECT DISTINCT pp.h3_index
    FROM bioprotect.project_pus pp
    JOIN bioprotect.projects pr ON pr.id = pp.project_id
    WHERE pr.planning_unit_id = p_planning_unit_id
  ) b ON b.h3_index = n::text
  WHERE n::text <> a.h3_index
  GROUP BY LEAST(a.h3_index, b.h3_index), GREATEST(a.h3_index, b.h3_index);

  GET DIAGNOSTICS v_count = ROW_COUNT;
  RETURN v_count;
END;
$$;


--
-- Name: prepare_prioritizr_input(bigint); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.prepare_prioritizr_input(p_run_id bigint) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
  v_project_id int;
  v_profile_id int;
  v_input_table text;
  v_feature_ids int[];
  v_feature_cols text[];
  v_select_feats text;
  v_sql text;
BEGIN
  -- 1) Resolve project
  SELECT project_id INTO v_project_id
  FROM bioprotect.prioritizr_runs
  WHERE id = p_run_id;

  IF v_project_id IS NULL THEN
    RAISE EXCEPTION 'Run % not found or missing project_id', p_run_id;
  END IF;

  -- 2) Resolve active cost profile
  SELECT active_cost_profile_id INTO v_profile_id
  FROM bioprotect.projects
  WHERE id = v_project_id;

  IF v_profile_id IS NULL THEN
    RAISE EXCEPTION 'Project % has no active_cost_profile_id', v_project_id;
  END IF;

  -- 3) Features in this project (your selected set)
  SELECT array_agg(pf.feature_unique_id ORDER BY pf.feature_unique_id)
  INTO v_feature_ids
  FROM bioprotect.project_features pf
  WHERE pf.project_id = v_project_id;

  IF v_feature_ids IS NULL OR array_length(v_feature_ids, 1) = 0 THEN
    RAISE EXCEPTION 'Project % has no project_features', v_project_id;
  END IF;

  -- 4) Name a per-run table (safe + unique)
  v_input_table := format('bioprotect.prioritizr_input_run_%s', p_run_id);

  -- 5) Build dynamic feature select list
  -- Each feature becomes: COALESCE(SUM(pfa.amount) FILTER (WHERE pfa.feature_unique_id = <fid>), 0) AS f_<fid>
  -- pfa.amount is already stored in km² (see insert_feature_pu_amounts).
  SELECT string_agg(
           format(
             'COALESCE(SUM(pfa.amount) FILTER (WHERE pfa.feature_unique_id = %s), 0)::double precision AS %I',
             fid, format('f_%s', fid)
           ),
           E',\n'
         )
  INTO v_select_feats
  FROM unnest(v_feature_ids) AS fid;

  SELECT array_agg(format('f_%s', fid)::text ORDER BY fid)
  INTO v_feature_cols
  FROM unnest(v_feature_ids) AS fid;

  -- 6) Drop any previous table for this run, then create UNLOGGED
  --    locked_in/locked_out derived from cost_profile_values.status:
  --    status 1 = locked_in, status 2 = locked_out
  v_sql := format($fmt$
    DROP TABLE IF EXISTS %s;
    CREATE UNLOGGED TABLE %s AS
    SELECT
      pp.h3_index::text AS pu_id,
      hc.geometry,
      cpv.cost::double precision AS cost,
      (ST_Area(hc.geometry::geography) / 1000000.0)::double precision AS area_km2,
      CASE WHEN cpv.status = 1 THEN 1 ELSE 0 END AS locked_in,
      CASE WHEN cpv.status = 2 THEN 1 ELSE 0 END AS locked_out,
      %s
    FROM bioprotect.project_pus pp
    JOIN bioprotect.h3_cells hc
      ON hc.h3_index = pp.h3_index
    JOIN bioprotect.cost_profile_values cpv
      ON cpv.project_pu_id = pp.id
     AND cpv.cost_profile_id = %s
    LEFT JOIN bioprotect.pu_feature_amounts pfa
      ON pfa.project_id = pp.project_id
     AND pfa.h3_index = pp.h3_index
     AND pfa.feature_unique_id = ANY(%L::int[])
    WHERE pp.project_id = %s
    GROUP BY pp.h3_index, hc.geometry, cpv.cost, cpv.status;
  $fmt$,
    v_input_table,
    v_input_table,
    v_select_feats,
    v_profile_id,
    v_feature_ids,
    v_project_id
  );

  EXECUTE v_sql;

  -- 7) Index for faster reads (optional but helpful)
  EXECUTE format('CREATE INDEX IF NOT EXISTS %I ON %s (pu_id);', 'idx_'||replace(v_input_table,'.','_')||'_pu', v_input_table);

  -- 8) Persist config on the run row
  UPDATE bioprotect.prioritizr_runs
  SET input_table = v_input_table,
      feature_cols = v_feature_cols,
      status = 'preparing'
  WHERE id = p_run_id;
END;
$_$;


--
-- Name: run_cumulative_impact(integer, integer[], text, text, text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.run_cumulative_impact(_project_id integer, _activity_ids integer[], _profile_name text, _description text DEFAULT ''::text, _user text DEFAULT 'system'::text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    _cost_profile INTEGER;
BEGIN
    -- ---------------------------------------------------------------
    -- Validate project has planning units
    -- ---------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM bioprotect.project_pus WHERE project_id = _project_id
    ) THEN
        RAISE EXCEPTION 'Project % has no planning units', _project_id;
    END IF;

    -- ---------------------------------------------------------------
    -- Create the cost profile
    -- ---------------------------------------------------------------
    INSERT INTO bioprotect.cost_profiles
        (project_id, name, description, created_by, is_default)
    VALUES
        (_project_id, _profile_name, _description, _user, false)
    RETURNING id INTO _cost_profile;

    -- ---------------------------------------------------------------
    -- Compute cumulative impact per hex and insert as cost values
    --
    -- Uses hex centroid containment instead of polygon intersection
    -- for performance. A hex is "covered" (coverage=1) if its centroid
    -- falls within the activity geometry, otherwise 0.
    --
    -- All pressures for a given activity share the same geometry, so
    -- we test containment once per (activity, hex) then fan out.
    -- ---------------------------------------------------------------
    WITH project_hexes AS (
        SELECT pp.id   AS project_pu_id,
               pp.h3_index,
               ST_Centroid(hc.geometry) AS hex_centroid
          FROM bioprotect.project_pus pp
          JOIN bioprotect.h3_cells hc
            ON hc.h3_index = pp.h3_index
         WHERE pp.project_id = _project_id
    ),
    -- Distinct activity geometries, simplified for performance
    activity_geoms AS (
        SELECT DISTINCT ON (activity_id)
               activity_id,
               ST_Simplify(geometry, 0.0001) AS geometry
          FROM bioprotect.pressures
         WHERE activity_id = ANY(_activity_ids)
    ),
    -- Binary containment: 1 if hex centroid is inside activity, else 0
    hex_activity_coverage AS (
        SELECT ph.project_pu_id,
               ph.h3_index,
               ag.activity_id,
               1::numeric AS pressure_coverage
          FROM project_hexes ph
          JOIN activity_geoms ag
            ON ST_Contains(ag.geometry, ph.hex_centroid)
    ),
    -- Fan out to pressures (no spatial ops)
    hex_pressures AS (
        SELECT hac.project_pu_id,
               hac.h3_index,
               p.pressuretitle,
               p.rppscore,
               hac.pressure_coverage
          FROM hex_activity_coverage hac
          JOIN bioprotect.pressures p
            ON p.activity_id = hac.activity_id
    ),
    hex_features AS (
        SELECT pfa.h3_index,
               mif.alias AS feature_name,
               pfa.amount AS feature_coverage
          FROM bioprotect.pu_feature_amounts pfa
          JOIN bioprotect.metadata_interest_features mif
            ON mif.unique_id = pfa.feature_unique_id
         WHERE pfa.project_id = _project_id
           AND pfa.amount > 0
    ),
    cumulative AS (
        SELECT hp.project_pu_id,
               SUM(
                   hp.pressure_coverage
                   * hp.rppscore
                   * hf.feature_coverage
                   * COALESCE(sm.sensitivity_score, 0)
               ) AS impact
          FROM hex_pressures hp
          JOIN hex_features hf
            ON hp.h3_index = hf.h3_index
          LEFT JOIN bioprotect.sensitivity_matrix sm
            ON sm.eunis_code  = hf.feature_name
           AND sm.pressure    = hp.pressuretitle
         GROUP BY hp.project_pu_id
    )
    -- Insert impacted hexes with normalised cost (0-1)
    INSERT INTO bioprotect.cost_profile_values
        (cost_profile_id, project_pu_id, cost, status)
    SELECT _cost_profile,
           c.project_pu_id,
           CASE
             WHEN max_impact.val > 0
             THEN c.impact / max_impact.val
             ELSE 0
           END,
           0
      FROM cumulative c,
           (SELECT MAX(impact) AS val FROM cumulative) max_impact;

    -- Fill non-impacted hexes with zero cost
    INSERT INTO bioprotect.cost_profile_values
        (cost_profile_id, project_pu_id, cost, status)
    SELECT _cost_profile, pp.id, 0, 0
      FROM bioprotect.project_pus pp
     WHERE pp.project_id = _project_id
       AND NOT EXISTS (
           SELECT 1 FROM bioprotect.cost_profile_values cpv
            WHERE cpv.cost_profile_id = _cost_profile
              AND cpv.project_pu_id   = pp.id
       );

    RETURN _cost_profile;
END;
$$;


--
-- Name: run_impact_pipeline(integer, integer[], text, text, text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.run_impact_pipeline(_project_id integer, _activity_ids integer[], _profile_name text, _description text DEFAULT ''::text, _user text DEFAULT 'system'::text) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    _aid            INTEGER;
    _pressure_count INTEGER;
    _cost_profile   INTEGER;
BEGIN
    -- Create pressures for each activity
    FOREACH _aid IN ARRAY _activity_ids LOOP
        SELECT bioprotect.create_pressures_from_activity(_aid)
          INTO _pressure_count;
        RAISE NOTICE 'Activity %: created % pressures', _aid, _pressure_count;
    END LOOP;

    -- Run cumulative impact
    SELECT bioprotect.run_cumulative_impact(
        _project_id, _activity_ids, _profile_name, _description, _user
    ) INTO _cost_profile;

    RETURN _cost_profile;
END;
$$;


--
-- Name: set_active_profile_pu_statuses(integer, text[], text[], text[]); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.set_active_profile_pu_statuses(p_project_id integer, p_status1_h3 text[] DEFAULT ARRAY[]::text[], p_status2_h3 text[] DEFAULT ARRAY[]::text[], p_status3_h3 text[] DEFAULT ARRAY[]::text[]) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_profile_id INT;
BEGIN
    -- Active profile must exist if status lives in profiles
    SELECT active_cost_profile_id
    INTO v_profile_id
    FROM bioprotect.projects
    WHERE id = p_project_id;

    IF v_profile_id IS NULL THEN
        RAISE EXCEPTION 'Project % has no active_cost_profile_id', p_project_id;
    END IF;

    -- Single-pass update using CASE
    UPDATE bioprotect.cost_profile_values cpv
    SET status =
        CASE
            WHEN pp.h3_index = ANY(p_status1_h3) THEN 1
            WHEN pp.h3_index = ANY(p_status2_h3) THEN 2
            WHEN pp.h3_index = ANY(p_status3_h3) THEN 3
            ELSE 0
        END
    FROM bioprotect.project_pus pp
    WHERE cpv.cost_profile_id = v_profile_id
      AND cpv.project_pu_id = pp.id
      AND pp.project_id = p_project_id;

END;
$$;


--
-- Name: square_grid(double precision, double precision, double precision, double precision, double precision); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.square_grid(areakm2 double precision, xmin double precision, ymin double precision, xmax double precision, ymax double precision) RETURNS SETOF public.geometry
    LANGUAGE plpgsql
    AS $$
DECLARE
	minpnt GEOMETRY;
	maxpnt GEOMETRY;
	minx FLOAT;
	miny FLOAT;
	maxx FLOAT;
	maxy FLOAT;
	xdistance FLOAT;
	sideLength FLOAT;
	xspacing FLOAT;
	yspacing FLOAT;
	xvertexlo FLOAT;
	xvertexhi FLOAT;
	rows INTEGER;
	columns INTEGER;
BEGIN
	-- Convert input coords to points in the 3410 projection
	minpnt = ST_Transform(ST_SetSRID(ST_MakePoint(xmin, ymin), 4326), 3410);
	maxpnt = ST_Transform(ST_SetSRID(ST_MakePoint(xmax, ymax), 4326), 3410);
	-- Get grid extents in 3410 projection
	minx = ST_X(minpnt);
	RAISE DEBUG 'minx: (%)', minx;
	miny = ST_Y(minpnt);
	RAISE DEBUG 'miny: (%)', miny;
	maxx = ST_X(maxpnt);
	RAISE DEBUG 'maxx: (%)', maxx;
	maxy = ST_Y(maxpnt);
	RAISE DEBUG 'maxy: (%)', maxy;
	
	-- Get the length of the square side
	sideLength = sqrt(areakm2 * 1000000.0);
	RAISE DEBUG 'sideLength: (%)', sideLength;

	--get the number of rows/columns
	rows = FLOOR((maxy - miny) / sideLength)::INTEGER;
	RAISE DEBUG 'crosses dateline: (%)', maxx<minx;
	IF (maxx<minx) THEN
		--get the x distance by adding the distance from minx to the dateline to the distance from the dateline to maxx
		xdistance := ST_Distance(minpnt, ST_Transform(ST_SetSRID(ST_Point(180,ymin),4326),3410)) + ST_Distance(ST_Transform(ST_SetSRID(ST_Point(-180,ymax),4326),3410),maxpnt);
		RAISE DEBUG 'xdistance: (%)', xdistance;
		columns = FLOOR(xdistance / sideLength)::INTEGER;
	ELSE
		columns = FLOOR((maxx - minx) / sideLength)::INTEGER;
	END IF;
	
	RAISE DEBUG 'rows: (%)', rows;
	RAISE DEBUG 'columns: (%)', columns;

	--create the squares/hexagons and return them
	RETURN QUERY 
		SELECT ST_SetSRID(ST_GeomFromText(format('POLYGON((%s %s, %s %s, %s %s, %s %s, %s %s))',x1,y1,x1,y2,x2,y2,x2,y1,x1,y1)), 3410) 
			FROM (SELECT minx + (c * sideLength) x1, minx + ((c + 1) * sideLength) x2, miny + (r * sideLength) y1, miny + ((r + 1) * sideLength) y2 FROM (SELECT c, r FROM generate_series(0, columns) AS c, generate_series(0, rows) AS r) AS sub) as points;
END
$$;


--
-- Name: st_splitatdateline(public.geometry); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.st_splitatdateline(feature public.geometry) RETURNS public.geometry
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: FUNCTION st_splitatdateline(feature public.geometry); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.st_splitatdateline(feature public.geometry) IS 'Splits the polygon feature into separate geometries if it crossed the dateline';


--
-- Name: touch_updated_at(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.touch_updated_at() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
begin new.updated_at = now(); return new; end $$;


--
-- Name: update_project_feature(integer, integer, text, numeric, numeric, numeric); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.update_project_feature(p_project_id integer, p_feature_id integer, p_target_type text, p_target_value numeric, p_spf numeric, p_weight numeric) RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    tv NUMERIC;
BEGIN
    -- 2) Normalize target value: convert % to proportion
    IF p_target_value IS NOT NULL THEN
        tv := p_target_value;
    ELSE
        tv := 0;
    END IF;

    -- 3) Upsert into project_features
    INSERT INTO bioprotect.project_features (
        project_id, feature_unique_id, target_type, target_value, spf, weight
    )
    VALUES (p_project_id, p_feature_id, p_target_type, tv, p_spf, p_weight)
    ON CONFLICT (project_id, feature_unique_id)
    DO UPDATE SET
        target_type  = COALESCE(EXCLUDED.target_type,  project_features.target_type),
        target_value = COALESCE(EXCLUDED.target_value, project_features.target_value),
        spf          = COALESCE(EXCLUDED.spf,          project_features.spf),
        weight       = COALESCE(EXCLUDED.weight,       project_features.weight),
        updated_at   = now();
END;
$$;


--
-- Name: updatefeaturesbatch(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.updatefeaturesbatch() RETURNS void
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: FUNCTION updatefeaturesbatch(); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.updatefeaturesbatch() IS 'Iterates through all the feature feature classes and updates them.';


--
-- Name: updateplanningunitsbatch(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.updateplanningunitsbatch() RETURNS void
    LANGUAGE plpgsql
    AS $$
DECLARE
    row     record;
	unitCount int;
BEGIN
    FOR row IN 
        SELECT feature_class_name FROM bioprotect.metadata_planning_units 
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
$$;


--
-- Name: FUNCTION updateplanningunitsbatch(); Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON FUNCTION bioprotect.updateplanningunitsbatch() IS 'Iterates through all the planning unit feature classes and extracts some information to populate in the metadata_planning_units table.';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: activity_0011a2c9745b489cb90400b; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.activity_0011a2c9745b489cb90400b (
    site_id character varying,
    bay character varying,
    harbour character varying,
    county character varying,
    site_statu character varying,
    licence_ty character varying,
    licensee_n character varying,
    aquacultur character varying,
    species_na character varying,
    species__1 character varying,
    species__2 character varying,
    species__3 character varying,
    species__4 character varying,
    species__5 character varying,
    species__6 character varying,
    species__7 character varying,
    species__8 character varying,
    species__9 character varying,
    species_co character varying,
    shape__len double precision,
    shape__are double precision,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: activity_0011a2c9745b489cb90400b_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.activity_0011a2c9745b489cb90400b_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: activity_0011a2c9745b489cb90400b_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.activity_0011a2c9745b489cb90400b_id_seq OWNED BY bioprotect.activity_0011a2c9745b489cb90400b.id;


--
-- Name: activity_2712fe1f88d942ac9cd34a5; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.activity_2712fe1f88d942ac9cd34a5 (
    gml_id character varying,
    objectid character varying,
    beginlifes character varying,
    endlifespa character varying,
    datasetnam character varying,
    localid character varying,
    inspireid character varying,
    inspirethe character varying,
    version character varying,
    descriptio character varying,
    geographic character varying,
    geograph_1 character varying,
    geograph_2 character varying,
    geograph_3 character varying,
    geartype character varying,
    species character varying,
    season character varying,
    daysperyea character varying,
    vessels character varying,
    loamax character varying,
    gearunits character varying,
    dataqualit character varying,
    scale character varying,
    coordinate character varying,
    stylelayer character varying,
    licence character varying,
    dataproven character varying,
    dataaccess character varying,
    area character varying,
    measureuni character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: activity_2712fe1f88d942ac9cd34a5_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.activity_2712fe1f88d942ac9cd34a5_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: activity_2712fe1f88d942ac9cd34a5_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.activity_2712fe1f88d942ac9cd34a5_id_seq OWNED BY bioprotect.activity_2712fe1f88d942ac9cd34a5.id;


--
-- Name: activity_37910b3f692d41959faa618; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.activity_37910b3f692d41959faa618 (
    gml_id character varying,
    objectid character varying,
    beginlifes character varying,
    endlifespa character varying,
    datasetnam character varying,
    localid character varying,
    inspireid character varying,
    inspirethe character varying,
    version character varying,
    descriptio character varying,
    geographic character varying,
    geograph_1 character varying,
    geograph_2 character varying,
    geograph_3 character varying,
    geartype character varying,
    species character varying,
    season character varying,
    daysperyea character varying,
    vessels character varying,
    loamax character varying,
    gearunits character varying,
    dataqualit character varying,
    scale character varying,
    coordinate character varying,
    stylelayer character varying,
    licence character varying,
    dataproven character varying,
    dataaccess character varying,
    area character varying,
    measureuni character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: activity_37910b3f692d41959faa618_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.activity_37910b3f692d41959faa618_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: activity_37910b3f692d41959faa618_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.activity_37910b3f692d41959faa618_id_seq OWNED BY bioprotect.activity_37910b3f692d41959faa618.id;


--
-- Name: activity_3f46079b31d04f16884545d_rid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.activity_3f46079b31d04f16884545d_rid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: activity_d45578c468bd4f12a91bd67; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.activity_d45578c468bd4f12a91bd67 (
    gml_id character varying,
    objectid character varying,
    beginlifes character varying,
    endlifespa character varying,
    datasetnam character varying,
    localid character varying,
    inspireid character varying,
    inspirethe character varying,
    version character varying,
    descriptio character varying,
    geographic character varying,
    geograph_1 character varying,
    geograph_2 character varying,
    geograph_3 character varying,
    geartype character varying,
    species character varying,
    season character varying,
    daysperyea character varying,
    vessels character varying,
    loamax character varying,
    gearunits character varying,
    dataqualit character varying,
    scale character varying,
    coordinate character varying,
    stylelayer character varying,
    licence character varying,
    dataproven character varying,
    dataaccess character varying,
    area character varying,
    measureuni character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: activity_d45578c468bd4f12a91bd67_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.activity_d45578c468bd4f12a91bd67_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: activity_d45578c468bd4f12a91bd67_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.activity_d45578c468bd4f12a91bd67_id_seq OWNED BY bioprotect.activity_d45578c468bd4f12a91bd67.id;


--
-- Name: activity_f939f8d4bee04500af1e172; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.activity_f939f8d4bee04500af1e172 (
    gml_id character varying,
    objectid character varying,
    beginlifes character varying,
    endlifespa character varying,
    datasetnam character varying,
    localid character varying,
    inspireid character varying,
    inspirethe character varying,
    version character varying,
    descriptio character varying,
    geographic character varying,
    geograph_1 character varying,
    geograph_2 character varying,
    geograph_3 character varying,
    geartype character varying,
    species character varying,
    season character varying,
    daysperyea character varying,
    vessels character varying,
    loamax character varying,
    gearunits character varying,
    dataqualit character varying,
    scale character varying,
    coordinate character varying,
    stylelayer character varying,
    licence character varying,
    dataproven character varying,
    dataaccess character varying,
    area character varying,
    measureuni character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: activity_f939f8d4bee04500af1e172_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.activity_f939f8d4bee04500af1e172_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: activity_f939f8d4bee04500af1e172_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.activity_f939f8d4bee04500af1e172_id_seq OWNED BY bioprotect.activity_f939f8d4bee04500af1e172.id;


--
-- Name: cost_profile_values; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.cost_profile_values (
    id integer NOT NULL,
    cost_profile_id integer NOT NULL,
    project_pu_id integer NOT NULL,
    cost numeric NOT NULL,
    status integer
);


--
-- Name: cost_profile_values_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.cost_profile_values_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cost_profile_values_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.cost_profile_values_id_seq OWNED BY bioprotect.cost_profile_values.id;


--
-- Name: cost_profiles; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.cost_profiles (
    id integer NOT NULL,
    project_id integer NOT NULL,
    name text NOT NULL,
    description text,
    created_by text,
    created_at timestamp with time zone DEFAULT now(),
    is_default boolean DEFAULT false
);


--
-- Name: cost_profiles_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.cost_profiles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cost_profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.cost_profiles_id_seq OWNED BY bioprotect.cost_profiles.id;


--
-- Name: eez_simplified_1km; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.eez_simplified_1km (
    ogc_fid integer NOT NULL,
    id_object numeric(10,0),
    id_country numeric(10,0),
    name_iso31 character varying(254),
    sovereign_ character varying(254),
    sovereig_1 character varying(254),
    sovereig_2 character varying(254),
    iso3 character varying(254),
    iso2 character varying(254),
    un_m49 character varying(254),
    source character varying(254),
    status character varying(254),
    original_d character varying(254),
    original_n character varying(254),
    source_cod character varying(254),
    orig_ogc_f numeric(10,0),
    shape_leng numeric(19,11),
    shape_area numeric(19,11),
    inpoly_fid numeric(10,0),
    simpgnflag numeric(5,0),
    maxsimptol numeric(19,11),
    minsimptol numeric(19,11),
    wkb_geometry public.geometry(MultiPolygon,4326)
);


--
-- Name: eez_simplified_1km_ogc_fid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.eez_simplified_1km_ogc_fid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: eez_simplified_1km_ogc_fid_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.eez_simplified_1km_ogc_fid_seq OWNED BY bioprotect.eez_simplified_1km.ogc_fid;


--
-- Name: f_01889e7f71624c0d9f2ad2c3241708; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_01889e7f71624c0d9f2ad2c3241708 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_01889e7f71624c0d9f2ad2c3241708_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_01889e7f71624c0d9f2ad2c3241708_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_01889e7f71624c0d9f2ad2c3241708_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_01889e7f71624c0d9f2ad2c3241708_id_seq OWNED BY bioprotect.f_01889e7f71624c0d9f2ad2c3241708.id;


--
-- Name: f_0b85bb11b41a4e269c0aebfbe0b544; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_0b85bb11b41a4e269c0aebfbe0b544_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_0b85bb11b41a4e269c0aebfbe0b544_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544_id_seq OWNED BY bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544.id;


--
-- Name: f_0c897459e69d4c22a1c00299aa5547; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_0c897459e69d4c22a1c00299aa5547 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_0c897459e69d4c22a1c00299aa5547_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_0c897459e69d4c22a1c00299aa5547_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_0c897459e69d4c22a1c00299aa5547_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_0c897459e69d4c22a1c00299aa5547_id_seq OWNED BY bioprotect.f_0c897459e69d4c22a1c00299aa5547.id;


--
-- Name: f_148a329e7dba44e8aea3c1151422ce; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_148a329e7dba44e8aea3c1151422ce (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_148a329e7dba44e8aea3c1151422ce_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_148a329e7dba44e8aea3c1151422ce_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_148a329e7dba44e8aea3c1151422ce_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_148a329e7dba44e8aea3c1151422ce_id_seq OWNED BY bioprotect.f_148a329e7dba44e8aea3c1151422ce.id;


--
-- Name: f_187731fcf47b4d2e982a38b5532a3a; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_187731fcf47b4d2e982a38b5532a3a (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_187731fcf47b4d2e982a38b5532a3a_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_187731fcf47b4d2e982a38b5532a3a_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_187731fcf47b4d2e982a38b5532a3a_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_187731fcf47b4d2e982a38b5532a3a_id_seq OWNED BY bioprotect.f_187731fcf47b4d2e982a38b5532a3a.id;


--
-- Name: f_1ca7a0ceec044faa9e153e72fce960; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_1ca7a0ceec044faa9e153e72fce960 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_1ca7a0ceec044faa9e153e72fce960_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_1ca7a0ceec044faa9e153e72fce960_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_1ca7a0ceec044faa9e153e72fce960_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_1ca7a0ceec044faa9e153e72fce960_id_seq OWNED BY bioprotect.f_1ca7a0ceec044faa9e153e72fce960.id;


--
-- Name: f_1f528ee9c1ee4a37a2e1ebbb1af2a9; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_1f528ee9c1ee4a37a2e1ebbb1af2a9_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_1f528ee9c1ee4a37a2e1ebbb1af2a9_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9_id_seq OWNED BY bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9.id;


--
-- Name: f_2187b5a975fe402e8f9a17cfa55eac; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_2187b5a975fe402e8f9a17cfa55eac (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_2187b5a975fe402e8f9a17cfa55eac_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_2187b5a975fe402e8f9a17cfa55eac_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_2187b5a975fe402e8f9a17cfa55eac_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_2187b5a975fe402e8f9a17cfa55eac_id_seq OWNED BY bioprotect.f_2187b5a975fe402e8f9a17cfa55eac.id;


--
-- Name: f_21debeb092f844f2b64b001bd64c29; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_21debeb092f844f2b64b001bd64c29 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_21debeb092f844f2b64b001bd64c29_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_21debeb092f844f2b64b001bd64c29_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_21debeb092f844f2b64b001bd64c29_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_21debeb092f844f2b64b001bd64c29_id_seq OWNED BY bioprotect.f_21debeb092f844f2b64b001bd64c29.id;


--
-- Name: f_234e530d24fc4252bed2b3e84ee1ba; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_234e530d24fc4252bed2b3e84ee1ba (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_234e530d24fc4252bed2b3e84ee1ba_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_234e530d24fc4252bed2b3e84ee1ba_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_234e530d24fc4252bed2b3e84ee1ba_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_234e530d24fc4252bed2b3e84ee1ba_id_seq OWNED BY bioprotect.f_234e530d24fc4252bed2b3e84ee1ba.id;


--
-- Name: f_245a2235f8d642269a1aaa82bedcb5; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_245a2235f8d642269a1aaa82bedcb5 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_245a2235f8d642269a1aaa82bedcb5_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_245a2235f8d642269a1aaa82bedcb5_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_245a2235f8d642269a1aaa82bedcb5_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_245a2235f8d642269a1aaa82bedcb5_id_seq OWNED BY bioprotect.f_245a2235f8d642269a1aaa82bedcb5.id;


--
-- Name: f_26a73e4c6eb0425e8c998ce0cb84b4; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_26a73e4c6eb0425e8c998ce0cb84b4_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_26a73e4c6eb0425e8c998ce0cb84b4_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4_id_seq OWNED BY bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4.id;


--
-- Name: f_2a587cadbd434a72b87ff6a2c3cc77; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_2a587cadbd434a72b87ff6a2c3cc77_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_2a587cadbd434a72b87ff6a2c3cc77_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77_id_seq OWNED BY bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77.id;


--
-- Name: f_2ccd7d878404486c8b8b9d5c90e9fc; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_2ccd7d878404486c8b8b9d5c90e9fc_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_2ccd7d878404486c8b8b9d5c90e9fc_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc_id_seq OWNED BY bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc.id;


--
-- Name: f_2ec291545abc47aea06b69f21e192c; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_2ec291545abc47aea06b69f21e192c (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_2ec291545abc47aea06b69f21e192c_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_2ec291545abc47aea06b69f21e192c_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_2ec291545abc47aea06b69f21e192c_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_2ec291545abc47aea06b69f21e192c_id_seq OWNED BY bioprotect.f_2ec291545abc47aea06b69f21e192c.id;


--
-- Name: f_2f5e67d4c1274b1eaf9a5a626a4282; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_2f5e67d4c1274b1eaf9a5a626a4282_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_2f5e67d4c1274b1eaf9a5a626a4282_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282_id_seq OWNED BY bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282.id;


--
-- Name: f_3061a71a8f0244848fbbff758c198e; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_3061a71a8f0244848fbbff758c198e (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_3061a71a8f0244848fbbff758c198e_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_3061a71a8f0244848fbbff758c198e_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_3061a71a8f0244848fbbff758c198e_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_3061a71a8f0244848fbbff758c198e_id_seq OWNED BY bioprotect.f_3061a71a8f0244848fbbff758c198e.id;


--
-- Name: f_352d7d971b674f06bcc2eb894c8685; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_352d7d971b674f06bcc2eb894c8685 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_352d7d971b674f06bcc2eb894c8685_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_352d7d971b674f06bcc2eb894c8685_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_352d7d971b674f06bcc2eb894c8685_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_352d7d971b674f06bcc2eb894c8685_id_seq OWNED BY bioprotect.f_352d7d971b674f06bcc2eb894c8685.id;


--
-- Name: f_37a68a8282d3495daee95f63fb8f6f; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_37a68a8282d3495daee95f63fb8f6f (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_37a68a8282d3495daee95f63fb8f6f_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_37a68a8282d3495daee95f63fb8f6f_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_37a68a8282d3495daee95f63fb8f6f_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_37a68a8282d3495daee95f63fb8f6f_id_seq OWNED BY bioprotect.f_37a68a8282d3495daee95f63fb8f6f.id;


--
-- Name: f_3923082fbb6e4b0bb22fbca0530c2f; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_3923082fbb6e4b0bb22fbca0530c2f_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_3923082fbb6e4b0bb22fbca0530c2f_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f_id_seq OWNED BY bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f.id;


--
-- Name: f_3bcc07d1d7e142ddbe995cee1c2060; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_3bcc07d1d7e142ddbe995cee1c2060_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_3bcc07d1d7e142ddbe995cee1c2060_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060_id_seq OWNED BY bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060.id;


--
-- Name: f_459758257ec542739ee8c64554920e; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_459758257ec542739ee8c64554920e (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_459758257ec542739ee8c64554920e_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_459758257ec542739ee8c64554920e_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_459758257ec542739ee8c64554920e_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_459758257ec542739ee8c64554920e_id_seq OWNED BY bioprotect.f_459758257ec542739ee8c64554920e.id;


--
-- Name: f_4dcb01bda5214973ae6d3d2f02982f; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_4dcb01bda5214973ae6d3d2f02982f (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_4dcb01bda5214973ae6d3d2f02982f_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_4dcb01bda5214973ae6d3d2f02982f_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_4dcb01bda5214973ae6d3d2f02982f_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_4dcb01bda5214973ae6d3d2f02982f_id_seq OWNED BY bioprotect.f_4dcb01bda5214973ae6d3d2f02982f.id;


--
-- Name: f_52d03f922eb14788ae1bef30f1429e; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_52d03f922eb14788ae1bef30f1429e (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_52d03f922eb14788ae1bef30f1429e_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_52d03f922eb14788ae1bef30f1429e_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_52d03f922eb14788ae1bef30f1429e_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_52d03f922eb14788ae1bef30f1429e_id_seq OWNED BY bioprotect.f_52d03f922eb14788ae1bef30f1429e.id;


--
-- Name: f_65f429eb64904e2a8c4d4f55a13cc9; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_65f429eb64904e2a8c4d4f55a13cc9_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_65f429eb64904e2a8c4d4f55a13cc9_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9_id_seq OWNED BY bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9.id;


--
-- Name: f_6deb40e7592e45f79fe1eb99a5f590; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_6deb40e7592e45f79fe1eb99a5f590 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_6deb40e7592e45f79fe1eb99a5f590_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_6deb40e7592e45f79fe1eb99a5f590_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_6deb40e7592e45f79fe1eb99a5f590_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_6deb40e7592e45f79fe1eb99a5f590_id_seq OWNED BY bioprotect.f_6deb40e7592e45f79fe1eb99a5f590.id;


--
-- Name: f_717bb7ba09ca4e38a18bdd04a88cc2; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_717bb7ba09ca4e38a18bdd04a88cc2_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_717bb7ba09ca4e38a18bdd04a88cc2_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2_id_seq OWNED BY bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2.id;


--
-- Name: f_7a5e4445f2a248d798a1e1a2b3d8c1; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_7a5e4445f2a248d798a1e1a2b3d8c1_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_7a5e4445f2a248d798a1e1a2b3d8c1_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1_id_seq OWNED BY bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1.id;


--
-- Name: f_7b6705b767494f6cb8c937e4929fd4; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_7b6705b767494f6cb8c937e4929fd4 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_7b6705b767494f6cb8c937e4929fd4_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_7b6705b767494f6cb8c937e4929fd4_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_7b6705b767494f6cb8c937e4929fd4_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_7b6705b767494f6cb8c937e4929fd4_id_seq OWNED BY bioprotect.f_7b6705b767494f6cb8c937e4929fd4.id;


--
-- Name: f_7c1b92efed5443b78dd4d0d09121c9; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_7c1b92efed5443b78dd4d0d09121c9 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_7c1b92efed5443b78dd4d0d09121c9_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_7c1b92efed5443b78dd4d0d09121c9_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_7c1b92efed5443b78dd4d0d09121c9_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_7c1b92efed5443b78dd4d0d09121c9_id_seq OWNED BY bioprotect.f_7c1b92efed5443b78dd4d0d09121c9.id;


--
-- Name: f_7eb4617384134a47982d7eee19769d; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_7eb4617384134a47982d7eee19769d (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_7eb4617384134a47982d7eee19769d_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_7eb4617384134a47982d7eee19769d_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_7eb4617384134a47982d7eee19769d_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_7eb4617384134a47982d7eee19769d_id_seq OWNED BY bioprotect.f_7eb4617384134a47982d7eee19769d.id;


--
-- Name: f_842802b2046c420b87a3d131633526; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_842802b2046c420b87a3d131633526 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_842802b2046c420b87a3d131633526_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_842802b2046c420b87a3d131633526_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_842802b2046c420b87a3d131633526_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_842802b2046c420b87a3d131633526_id_seq OWNED BY bioprotect.f_842802b2046c420b87a3d131633526.id;


--
-- Name: f_88a663104e2e4eb58f77c06d2c2480; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_88a663104e2e4eb58f77c06d2c2480 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_88a663104e2e4eb58f77c06d2c2480_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_88a663104e2e4eb58f77c06d2c2480_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_88a663104e2e4eb58f77c06d2c2480_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_88a663104e2e4eb58f77c06d2c2480_id_seq OWNED BY bioprotect.f_88a663104e2e4eb58f77c06d2c2480.id;


--
-- Name: f_8e49e2ec060746578e1fec042d6565; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_8e49e2ec060746578e1fec042d6565 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_8e49e2ec060746578e1fec042d6565_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_8e49e2ec060746578e1fec042d6565_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_8e49e2ec060746578e1fec042d6565_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_8e49e2ec060746578e1fec042d6565_id_seq OWNED BY bioprotect.f_8e49e2ec060746578e1fec042d6565.id;


--
-- Name: f_9081d33eaa78434bbee51ed915a94c; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_9081d33eaa78434bbee51ed915a94c (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_9081d33eaa78434bbee51ed915a94c_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_9081d33eaa78434bbee51ed915a94c_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_9081d33eaa78434bbee51ed915a94c_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_9081d33eaa78434bbee51ed915a94c_id_seq OWNED BY bioprotect.f_9081d33eaa78434bbee51ed915a94c.id;


--
-- Name: f_91ecb37cc23e4c8a86fa08c9902d80; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_91ecb37cc23e4c8a86fa08c9902d80_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_91ecb37cc23e4c8a86fa08c9902d80_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80_id_seq OWNED BY bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80.id;


--
-- Name: f_99e1849671054f0eb34effaefe2064; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_99e1849671054f0eb34effaefe2064 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_99e1849671054f0eb34effaefe2064_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_99e1849671054f0eb34effaefe2064_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_99e1849671054f0eb34effaefe2064_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_99e1849671054f0eb34effaefe2064_id_seq OWNED BY bioprotect.f_99e1849671054f0eb34effaefe2064.id;


--
-- Name: f_a24524f50a444a4689645403db2ef8; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_a24524f50a444a4689645403db2ef8 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_a24524f50a444a4689645403db2ef8_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_a24524f50a444a4689645403db2ef8_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_a24524f50a444a4689645403db2ef8_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_a24524f50a444a4689645403db2ef8_id_seq OWNED BY bioprotect.f_a24524f50a444a4689645403db2ef8.id;


--
-- Name: f_a3da4861d02a4adba10e55b9ab9e6e; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_a3da4861d02a4adba10e55b9ab9e6e_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_a3da4861d02a4adba10e55b9ab9e6e_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e_id_seq OWNED BY bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e.id;


--
-- Name: f_a5e612cd6d394f28802adebcac250a; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_a5e612cd6d394f28802adebcac250a (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_a5e612cd6d394f28802adebcac250a_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_a5e612cd6d394f28802adebcac250a_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_a5e612cd6d394f28802adebcac250a_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_a5e612cd6d394f28802adebcac250a_id_seq OWNED BY bioprotect.f_a5e612cd6d394f28802adebcac250a.id;


--
-- Name: f_a7d1c02731ec419082a7277fe13cc0; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_a7d1c02731ec419082a7277fe13cc0 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_a7d1c02731ec419082a7277fe13cc0_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_a7d1c02731ec419082a7277fe13cc0_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_a7d1c02731ec419082a7277fe13cc0_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_a7d1c02731ec419082a7277fe13cc0_id_seq OWNED BY bioprotect.f_a7d1c02731ec419082a7277fe13cc0.id;


--
-- Name: f_aef913e8ecd147299348eb5e9a629f; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_aef913e8ecd147299348eb5e9a629f (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_aef913e8ecd147299348eb5e9a629f_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_aef913e8ecd147299348eb5e9a629f_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_aef913e8ecd147299348eb5e9a629f_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_aef913e8ecd147299348eb5e9a629f_id_seq OWNED BY bioprotect.f_aef913e8ecd147299348eb5e9a629f.id;


--
-- Name: f_b1d980c8b69441dc9ff24b14237f11; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b1d980c8b69441dc9ff24b14237f11 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_b1d980c8b69441dc9ff24b14237f11_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b1d980c8b69441dc9ff24b14237f11_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b1d980c8b69441dc9ff24b14237f11_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b1d980c8b69441dc9ff24b14237f11_id_seq OWNED BY bioprotect.f_b1d980c8b69441dc9ff24b14237f11.id;


--
-- Name: f_b25a913184af4c39bd069076ccee9c; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b25a913184af4c39bd069076ccee9c (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_b25a913184af4c39bd069076ccee9c_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b25a913184af4c39bd069076ccee9c_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b25a913184af4c39bd069076ccee9c_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b25a913184af4c39bd069076ccee9c_id_seq OWNED BY bioprotect.f_b25a913184af4c39bd069076ccee9c.id;


--
-- Name: f_b3491f0025d34f0194be96e4547e36; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b3491f0025d34f0194be96e4547e36 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_b3491f0025d34f0194be96e4547e36_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b3491f0025d34f0194be96e4547e36_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b3491f0025d34f0194be96e4547e36_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b3491f0025d34f0194be96e4547e36_id_seq OWNED BY bioprotect.f_b3491f0025d34f0194be96e4547e36.id;


--
-- Name: f_b3d1f9032ad243d18e768a8d7e3f76; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_b3d1f9032ad243d18e768a8d7e3f76_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b3d1f9032ad243d18e768a8d7e3f76_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76_id_seq OWNED BY bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76.id;


--
-- Name: f_b4d2b4619455408e9ba0f02386e539; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b4d2b4619455408e9ba0f02386e539 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_b4d2b4619455408e9ba0f02386e539_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b4d2b4619455408e9ba0f02386e539_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b4d2b4619455408e9ba0f02386e539_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b4d2b4619455408e9ba0f02386e539_id_seq OWNED BY bioprotect.f_b4d2b4619455408e9ba0f02386e539.id;


--
-- Name: f_b4e74e66d4bf4a428824a10431e0d7; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b4e74e66d4bf4a428824a10431e0d7 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_b4e74e66d4bf4a428824a10431e0d7_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b4e74e66d4bf4a428824a10431e0d7_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b4e74e66d4bf4a428824a10431e0d7_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b4e74e66d4bf4a428824a10431e0d7_id_seq OWNED BY bioprotect.f_b4e74e66d4bf4a428824a10431e0d7.id;


--
-- Name: f_b7052d76f72148b9aecbf08f7d300f; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b7052d76f72148b9aecbf08f7d300f (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_b7052d76f72148b9aecbf08f7d300f_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b7052d76f72148b9aecbf08f7d300f_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b7052d76f72148b9aecbf08f7d300f_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b7052d76f72148b9aecbf08f7d300f_id_seq OWNED BY bioprotect.f_b7052d76f72148b9aecbf08f7d300f.id;


--
-- Name: f_b7dc2be4209a496aa66e85f457a443; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b7dc2be4209a496aa66e85f457a443 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_b7dc2be4209a496aa66e85f457a443_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b7dc2be4209a496aa66e85f457a443_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b7dc2be4209a496aa66e85f457a443_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b7dc2be4209a496aa66e85f457a443_id_seq OWNED BY bioprotect.f_b7dc2be4209a496aa66e85f457a443.id;


--
-- Name: f_b9d3d3026af049c2a22e3bb79a3869; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b9d3d3026af049c2a22e3bb79a3869 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_b9d3d3026af049c2a22e3bb79a3869_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b9d3d3026af049c2a22e3bb79a3869_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b9d3d3026af049c2a22e3bb79a3869_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b9d3d3026af049c2a22e3bb79a3869_id_seq OWNED BY bioprotect.f_b9d3d3026af049c2a22e3bb79a3869.id;


--
-- Name: f_b9f108f222074be5b2d7bfac0705b3; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_b9f108f222074be5b2d7bfac0705b3 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_b9f108f222074be5b2d7bfac0705b3_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_b9f108f222074be5b2d7bfac0705b3_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_b9f108f222074be5b2d7bfac0705b3_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_b9f108f222074be5b2d7bfac0705b3_id_seq OWNED BY bioprotect.f_b9f108f222074be5b2d7bfac0705b3.id;


--
-- Name: f_bd44b7714c7f4dc2be4ab9c23e44c3; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_bd44b7714c7f4dc2be4ab9c23e44c3_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_bd44b7714c7f4dc2be4ab9c23e44c3_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3_id_seq OWNED BY bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3.id;


--
-- Name: f_bf7c637287f149348dbe268887459a; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_bf7c637287f149348dbe268887459a (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_bf7c637287f149348dbe268887459a_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_bf7c637287f149348dbe268887459a_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_bf7c637287f149348dbe268887459a_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_bf7c637287f149348dbe268887459a_id_seq OWNED BY bioprotect.f_bf7c637287f149348dbe268887459a.id;


--
-- Name: f_c00802c241d6413b8b1bc15677a816; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_c00802c241d6413b8b1bc15677a816 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_c00802c241d6413b8b1bc15677a816_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_c00802c241d6413b8b1bc15677a816_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_c00802c241d6413b8b1bc15677a816_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_c00802c241d6413b8b1bc15677a816_id_seq OWNED BY bioprotect.f_c00802c241d6413b8b1bc15677a816.id;


--
-- Name: f_c275409be23c4ffcae31cf9346077e; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_c275409be23c4ffcae31cf9346077e (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_c275409be23c4ffcae31cf9346077e_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_c275409be23c4ffcae31cf9346077e_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_c275409be23c4ffcae31cf9346077e_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_c275409be23c4ffcae31cf9346077e_id_seq OWNED BY bioprotect.f_c275409be23c4ffcae31cf9346077e.id;


--
-- Name: f_c6c00aba44cd4932a704eb64605d30; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_c6c00aba44cd4932a704eb64605d30 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_c6c00aba44cd4932a704eb64605d30_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_c6c00aba44cd4932a704eb64605d30_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_c6c00aba44cd4932a704eb64605d30_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_c6c00aba44cd4932a704eb64605d30_id_seq OWNED BY bioprotect.f_c6c00aba44cd4932a704eb64605d30.id;


--
-- Name: f_cb5c84470af44206942d56e73f2537; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_cb5c84470af44206942d56e73f2537 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_cb5c84470af44206942d56e73f2537_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_cb5c84470af44206942d56e73f2537_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_cb5c84470af44206942d56e73f2537_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_cb5c84470af44206942d56e73f2537_id_seq OWNED BY bioprotect.f_cb5c84470af44206942d56e73f2537.id;


--
-- Name: f_cb7eed03815440278883a83e21d29a; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_cb7eed03815440278883a83e21d29a (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_cb7eed03815440278883a83e21d29a_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_cb7eed03815440278883a83e21d29a_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_cb7eed03815440278883a83e21d29a_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_cb7eed03815440278883a83e21d29a_id_seq OWNED BY bioprotect.f_cb7eed03815440278883a83e21d29a.id;


--
-- Name: f_cd23e938324b47699975c875199bc9; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_cd23e938324b47699975c875199bc9 (
    origname character varying,
    "eunis 2022" character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_cd23e938324b47699975c875199bc9_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_cd23e938324b47699975c875199bc9_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_cd23e938324b47699975c875199bc9_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_cd23e938324b47699975c875199bc9_id_seq OWNED BY bioprotect.f_cd23e938324b47699975c875199bc9.id;


--
-- Name: f_cef1ce78dbf649c2a5b936227217db; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_cef1ce78dbf649c2a5b936227217db (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_cef1ce78dbf649c2a5b936227217db_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_cef1ce78dbf649c2a5b936227217db_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_cef1ce78dbf649c2a5b936227217db_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_cef1ce78dbf649c2a5b936227217db_id_seq OWNED BY bioprotect.f_cef1ce78dbf649c2a5b936227217db.id;


--
-- Name: f_dd21a32dab4d4971851c4ed1f7aae9; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_dd21a32dab4d4971851c4ed1f7aae9_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_dd21a32dab4d4971851c4ed1f7aae9_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9_id_seq OWNED BY bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9.id;


--
-- Name: f_ddc3ac5aa4c54101bf2ac019495a37; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_ddc3ac5aa4c54101bf2ac019495a37_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_ddc3ac5aa4c54101bf2ac019495a37_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37_id_seq OWNED BY bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37.id;


--
-- Name: f_e65e35e598e243419bcb96e759b2eb; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_e65e35e598e243419bcb96e759b2eb (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_e65e35e598e243419bcb96e759b2eb_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_e65e35e598e243419bcb96e759b2eb_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_e65e35e598e243419bcb96e759b2eb_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_e65e35e598e243419bcb96e759b2eb_id_seq OWNED BY bioprotect.f_e65e35e598e243419bcb96e759b2eb.id;


--
-- Name: f_e93241238a0f4153a9c8d648e55662; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_e93241238a0f4153a9c8d648e55662 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_e93241238a0f4153a9c8d648e55662_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_e93241238a0f4153a9c8d648e55662_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_e93241238a0f4153a9c8d648e55662_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_e93241238a0f4153a9c8d648e55662_id_seq OWNED BY bioprotect.f_e93241238a0f4153a9c8d648e55662.id;


--
-- Name: f_ea9d7de99bb2447986b4b4849eeb81; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_ea9d7de99bb2447986b4b4849eeb81 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_ea9d7de99bb2447986b4b4849eeb81_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_ea9d7de99bb2447986b4b4849eeb81_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_ea9d7de99bb2447986b4b4849eeb81_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_ea9d7de99bb2447986b4b4849eeb81_id_seq OWNED BY bioprotect.f_ea9d7de99bb2447986b4b4849eeb81.id;


--
-- Name: f_f22a1a11a1134a86ab6e32ad52ccf3; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_f22a1a11a1134a86ab6e32ad52ccf3_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_f22a1a11a1134a86ab6e32ad52ccf3_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3_id_seq OWNED BY bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3.id;


--
-- Name: f_f2b98a9b43ac45778f17d27444ad6d; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_f2b98a9b43ac45778f17d27444ad6d (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_f2b98a9b43ac45778f17d27444ad6d_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_f2b98a9b43ac45778f17d27444ad6d_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_f2b98a9b43ac45778f17d27444ad6d_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_f2b98a9b43ac45778f17d27444ad6d_id_seq OWNED BY bioprotect.f_f2b98a9b43ac45778f17d27444ad6d.id;


--
-- Name: f_f5431315d3d149d2a707d5c829b703; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_f5431315d3d149d2a707d5c829b703 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_f5431315d3d149d2a707d5c829b703_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_f5431315d3d149d2a707d5c829b703_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_f5431315d3d149d2a707d5c829b703_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_f5431315d3d149d2a707d5c829b703_id_seq OWNED BY bioprotect.f_f5431315d3d149d2a707d5c829b703.id;


--
-- Name: f_f782eda898d94749947dd8ba1ced20; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_f782eda898d94749947dd8ba1ced20 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_f782eda898d94749947dd8ba1ced20_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_f782eda898d94749947dd8ba1ced20_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_f782eda898d94749947dd8ba1ced20_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_f782eda898d94749947dd8ba1ced20_id_seq OWNED BY bioprotect.f_f782eda898d94749947dd8ba1ced20.id;


--
-- Name: f_f8dfa6923e2d46a5afb5cb3c778894; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_f8dfa6923e2d46a5afb5cb3c778894_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_f8dfa6923e2d46a5afb5cb3c778894_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894_id_seq OWNED BY bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894.id;


--
-- Name: f_f9e0fe869ca14d4ebf8cf503df4ea5; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_f9e0fe869ca14d4ebf8cf503df4ea5_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_f9e0fe869ca14d4ebf8cf503df4ea5_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5_id_seq OWNED BY bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5.id;


--
-- Name: f_fb1438f3390a438d96a21b870e3319; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_fb1438f3390a438d96a21b870e3319 (
    euniscombd text,
    msfd_bbht text,
    unique_eun text,
    density numeric,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: f_fb1438f3390a438d96a21b870e3319_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_fb1438f3390a438d96a21b870e3319_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_fb1438f3390a438d96a21b870e3319_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_fb1438f3390a438d96a21b870e3319_id_seq OWNED BY bioprotect.f_fb1438f3390a438d96a21b870e3319.id;


--
-- Name: f_fba08cfc6e534e778af5888e6cabd6; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_fba08cfc6e534e778af5888e6cabd6 (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_fba08cfc6e534e778af5888e6cabd6_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_fba08cfc6e534e778af5888e6cabd6_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_fba08cfc6e534e778af5888e6cabd6_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_fba08cfc6e534e778af5888e6cabd6_id_seq OWNED BY bioprotect.f_fba08cfc6e534e778af5888e6cabd6.id;


--
-- Name: f_fd18eeb663824a85aa6b05987c781c; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.f_fd18eeb663824a85aa6b05987c781c (
    origname character varying,
    eunis_2022 character varying,
    simplename character varying,
    geometry public.geometry(Geometry,4326),
    id integer NOT NULL
);


--
-- Name: f_fd18eeb663824a85aa6b05987c781c_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.f_fd18eeb663824a85aa6b05987c781c_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: f_fd18eeb663824a85aa6b05987c781c_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.f_fd18eeb663824a85aa6b05987c781c_id_seq OWNED BY bioprotect.f_fd18eeb663824a85aa6b05987c781c.id;


--
-- Name: feature_preprocessing; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.feature_preprocessing (
    project_id integer NOT NULL,
    feature_unique_id integer NOT NULL,
    pu_area double precision,
    pu_count integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);


--
-- Name: feature_preprocessing_backup_km2migration; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.feature_preprocessing_backup_km2migration (
    project_id integer,
    feature_unique_id integer,
    pu_area double precision,
    pu_count integer,
    created_at timestamp with time zone,
    updated_at timestamp with time zone
);


--
-- Name: gap_admin_start_project; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gap_admin_start_project (
    feature_class_name text,
    alias text,
    total_area double precision,
    country_area double precision,
    current_protected_area double precision,
    current_protected_percent double precision
);


--
-- Name: gaul_2015_simplified_1km; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gaul_2015_simplified_1km (
    ogc_fid integer NOT NULL,
    id_object numeric(10,0),
    id_country numeric(10,0),
    name_iso31 character varying(254),
    sovereign_ character varying(254),
    sovereig_1 character varying(254),
    sovereig_2 character varying(254),
    iso3 character varying(254),
    iso2 character varying(254),
    un_m49 character varying(254),
    source character varying(254),
    status character varying(254),
    original_d character varying(254),
    original_n character varying(254),
    source_cod character varying(254),
    orig_ogc_f numeric(10,0),
    area_km2 numeric(19,11),
    shape_leng numeric(19,11),
    shape_area numeric(19,11),
    inpoly_fid numeric(10,0),
    simpgnflag numeric(5,0),
    maxsimptol numeric(19,11),
    minsimptol numeric(19,11),
    wkb_geometry public.geometry(Geometry,4326)
);


--
-- Name: gaul_2015_simplified_1km_ogc_fid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gaul_2015_simplified_1km_ogc_fid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gaul_2015_simplified_1km_ogc_fid_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.gaul_2015_simplified_1km_ogc_fid_seq OWNED BY bioprotect.gaul_2015_simplified_1km.ogc_fid;


--
-- Name: gaul_eez_dissolved; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gaul_eez_dissolved (
    country_id integer NOT NULL,
    geom public.geometry(MultiPolygon,4326),
    country_name text,
    iso3 text,
    iso2 text,
    un_m49 text,
    status text,
    sqkm double precision,
    geom_mollweide public.geometry
);


--
-- Name: TABLE gaul_eez_dissolved; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON TABLE bioprotect.gaul_eez_dissolved IS 'last version of gaul_eez_dissolved|TRUE';


--
-- Name: COLUMN gaul_eez_dissolved.geom_mollweide; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON COLUMN bioprotect.gaul_eez_dissolved.geom_mollweide IS 'Geometry projected to mollweide';


--
-- Name: gbif_2480623; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gbif_2480623 (
    eventdate date,
    gbifid bigint,
    lng double precision,
    lat double precision,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: gbif_2480623_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gbif_2480623_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gbif_2480623_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.gbif_2480623_id_seq OWNED BY bioprotect.gbif_2480623.id;


--
-- Name: gbif_2486629; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gbif_2486629 (
    eventdate date,
    gbifid bigint,
    lng double precision,
    lat double precision,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: gbif_2486629_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gbif_2486629_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gbif_2486629_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.gbif_2486629_id_seq OWNED BY bioprotect.gbif_2486629.id;


--
-- Name: gbif_2486630; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gbif_2486630 (
    eventdate date,
    gbifid bigint,
    lng double precision,
    lat double precision,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: gbif_2486630_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gbif_2486630_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gbif_2486630_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.gbif_2486630_id_seq OWNED BY bioprotect.gbif_2486630.id;


--
-- Name: gbif_2495255; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gbif_2495255 (
    eventdate date,
    gbifid bigint,
    lng double precision,
    lat double precision,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: gbif_2495255_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gbif_2495255_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gbif_2495255_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.gbif_2495255_id_seq OWNED BY bioprotect.gbif_2495255.id;


--
-- Name: gbif_5230455; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gbif_5230455 (
    eventdate date,
    gbifid bigint,
    lng double precision,
    lat double precision,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: gbif_5230455_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gbif_5230455_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gbif_5230455_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.gbif_5230455_id_seq OWNED BY bioprotect.gbif_5230455.id;


--
-- Name: gbif_9056437; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.gbif_9056437 (
    eventdate date,
    gbifid bigint,
    lng double precision,
    lat double precision,
    geometry public.geometry,
    id integer NOT NULL
);


--
-- Name: gbif_9056437_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gbif_9056437_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: gbif_9056437_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.gbif_9056437_id_seq OWNED BY bioprotect.gbif_9056437.id;


--
-- Name: grid_boundary_edges; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.grid_boundary_edges (
    planning_unit_id integer NOT NULL,
    h3_a text NOT NULL,
    h3_b text NOT NULL,
    boundary double precision DEFAULT 1.0 NOT NULL
);


--
-- Name: h3_cells; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.h3_cells (
    h3_index text NOT NULL,
    resolution integer NOT NULL,
    scale_level text NOT NULL,
    project_area text NOT NULL,
    geometry public.geometry(Polygon,4326),
    geom public.geometry
);


--
-- Name: ices_ecoregions; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.ices_ecoregions (
    ogc_fid integer NOT NULL,
    objectid numeric(10,0),
    ecoregion character varying(50),
    shape_leng numeric(18,11),
    shape_le_1 numeric(18,11),
    shape_area numeric(18,11),
    geometry public.geometry(MultiPolygon,4326)
);


--
-- Name: ices_ecoregions_ogc_fid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.ices_ecoregions_ogc_fid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: ices_ecoregions_ogc_fid_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.ices_ecoregions_ogc_fid_seq OWNED BY bioprotect.ices_ecoregions.ogc_fid;


--
-- Name: impact_e0db42b42ced448fafbb6d66a_rid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.impact_e0db42b42ced448fafbb6d66a_rid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: jncc_sensitivities; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.jncc_sensitivities (
    id integer NOT NULL,
    eunis_code_assessment text,
    habitat_assessment text,
    eunis_code_avail text,
    jncc_habitat text,
    jncc_pressure text,
    maresa_pressure text,
    resistance text,
    resistance_alt text,
    resilience text,
    resilience_alt text,
    sensitivity text,
    sensitivity_confidence text,
    evidence text
);


--
-- Name: jncc_sensitivities_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.jncc_sensitivities_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: jncc_sensitivities_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.jncc_sensitivities_id_seq OWNED BY bioprotect.jncc_sensitivities.id;


--
-- Name: metadata_activities; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.metadata_activities (
    description text,
    creation_date timestamp without time zone,
    source text,
    created_by text,
    filename character varying,
    activity character varying,
    activity_name character varying,
    extent public.box2d,
    id integer NOT NULL,
    upload_type text DEFAULT 'raster'::text,
    CONSTRAINT metadata_activities_upload_type_check CHECK ((upload_type = ANY (ARRAY['raster'::text, 'shapefile'::text])))
);


--
-- Name: metadata_activities_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.metadata_activities_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: metadata_activities_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.metadata_activities_id_seq OWNED BY bioprotect.metadata_activities.id;


--
-- Name: metadata_impacts; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.metadata_impacts (
    feature_class_name text,
    alias text,
    description text,
    creation_date timestamp without time zone,
    tilesetid text,
    extent public.box2d,
    source text,
    created_by text,
    id integer NOT NULL
);


--
-- Name: metadata_impacts_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.metadata_impacts_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: metadata_impacts_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.metadata_impacts_id_seq OWNED BY bioprotect.metadata_impacts.id;


--
-- Name: metadata_interest_features; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.metadata_interest_features (
    unique_id integer NOT NULL,
    feature_class_name text,
    alias text,
    description text,
    creation_date timestamp without time zone,
    _area double precision,
    tilesetid text,
    extent public.box2d,
    source text,
    created_by text
);


--
-- Name: TABLE metadata_interest_features; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON TABLE bioprotect.metadata_interest_features IS 'Holds metadata information on the Marxan interest features';


--
-- Name: COLUMN metadata_interest_features.feature_class_name; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON COLUMN bioprotect.metadata_interest_features.feature_class_name IS 'The name of the feature class that the spatial data is held in';


--
-- Name: COLUMN metadata_interest_features.alias; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON COLUMN bioprotect.metadata_interest_features.alias IS 'An alias or display name for the data';


--
-- Name: COLUMN metadata_interest_features.tilesetid; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON COLUMN bioprotect.metadata_interest_features.tilesetid IS 'Mapbox tilesetId if the feature class has been uploaded to Mapbox';


--
-- Name: COLUMN metadata_interest_features.source; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON COLUMN bioprotect.metadata_interest_features.source IS 'How the feature was created';


--
-- Name: metadata_interest_features_unique_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.metadata_interest_features_unique_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: metadata_interest_features_unique_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.metadata_interest_features_unique_id_seq OWNED BY bioprotect.metadata_interest_features.unique_id;


--
-- Name: metadata_planning_units; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.metadata_planning_units (
    unique_id integer NOT NULL,
    feature_class_name text,
    alias text,
    description text,
    country_id integer,
    aoi_id integer,
    domain text,
    _area double precision,
    envelope public.geometry(MultiPolygon,4326),
    creation_date timestamp without time zone,
    source text,
    created_by text,
    tilesetid text,
    planning_unit_count integer
);


--
-- Name: COLUMN metadata_planning_units.source; Type: COMMENT; Schema: bioprotect; Owner: -
--

COMMENT ON COLUMN bioprotect.metadata_planning_units.source IS 'How the planning grid was created';


--
-- Name: metadata_planning_units_unique_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.metadata_planning_units_unique_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: metadata_planning_units_unique_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.metadata_planning_units_unique_id_seq OWNED BY bioprotect.metadata_planning_units.unique_id;


--
-- Name: pad; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.pad (
    index bigint,
    " " bigint,
    categorytitle text,
    activitytitle text,
    pressuretitle text,
    apjustificationdesc text,
    rpptitle text,
    riskfactordesc text,
    evidencestandarddesc text,
    confidencescoretitle text,
    rppscore double precision
);


--
-- Name: pressures; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.pressures (
    id integer NOT NULL,
    activity_id integer,
    pressuretitle text NOT NULL,
    rppscore numeric NOT NULL,
    geometry public.geometry(Geometry,4326) NOT NULL,
    created_at timestamp without time zone DEFAULT now()
);


--
-- Name: pressures_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.pressures_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: pressures_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.pressures_id_seq OWNED BY bioprotect.pressures.id;


--
-- Name: prioritizr_input_run_1; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_1 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_10; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_10 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_11; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_11 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_12; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_12 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_13; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_13 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_14; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_14 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_16; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_16 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_125 double precision,
    f_126 double precision,
    f_127 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_2383190 double precision
);


--
-- Name: prioritizr_input_run_17; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_17 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_125 double precision,
    f_126 double precision,
    f_127 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_2383190 double precision
);


--
-- Name: prioritizr_input_run_18; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_18 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_125 double precision,
    f_126 double precision,
    f_127 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_2383190 double precision
);


--
-- Name: prioritizr_input_run_19; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_19 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_125 double precision,
    f_126 double precision,
    f_127 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_2; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_2 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_20; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_20 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_125 double precision,
    f_126 double precision,
    f_127 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_21; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_21 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_109 double precision,
    f_110 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_139 double precision,
    f_148 double precision,
    f_152 double precision,
    f_155 double precision,
    f_157 double precision
);


--
-- Name: prioritizr_input_run_22; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_22 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_109 double precision,
    f_110 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_148 double precision
);


--
-- Name: prioritizr_input_run_23; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_23 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_109 double precision,
    f_110 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_148 double precision
);


--
-- Name: prioritizr_input_run_24; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_24 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_109 double precision,
    f_110 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_148 double precision
);


--
-- Name: prioritizr_input_run_25; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_25 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_109 double precision,
    f_110 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision,
    f_148 double precision
);


--
-- Name: prioritizr_input_run_3; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_3 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_31; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_31 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_108 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_32; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_32 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_108 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_33; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_33 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_34; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_34 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_35; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_35 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_36; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_36 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_37; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_37 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_38; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_38 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_39; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_39 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_4; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_4 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_40; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_40 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_41; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_41 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_42; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_42 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_43; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_43 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_44; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_44 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_45; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_45 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_46; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_46 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_47; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_47 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_48; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_48 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_49; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_49 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_5; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_5 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_50; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_50 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_51; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_51 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_52; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_52 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_106 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_53; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_53 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_54; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_54 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_55; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_55 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_56; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_56 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_57; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_57 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_58; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_58 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_59; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_59 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_6; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_6 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_60; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_60 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_61; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_61 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_62; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_62 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_63; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_63 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_64; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_64 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_65; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_65 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_66; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_66 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_67; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_67 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_68; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_68 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_69; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_69 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_7; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_7 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_70; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_70 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_71; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_71 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_131 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_72; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_72 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_73; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_73 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision,
    f_158 double precision,
    f_160 double precision
);


--
-- Name: prioritizr_input_run_74; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_74 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision,
    f_158 double precision,
    f_160 double precision
);


--
-- Name: prioritizr_input_run_75; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_75 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision,
    f_158 double precision,
    f_160 double precision
);


--
-- Name: prioritizr_input_run_76; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_76 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision,
    f_158 double precision,
    f_160 double precision
);


--
-- Name: prioritizr_input_run_77; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_77 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_92 double precision,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_78; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_78 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_92 double precision,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_79; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_79 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_92 double precision,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_8; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_8 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_input_run_80; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_80 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_92 double precision,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision
);


--
-- Name: prioritizr_input_run_81; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_81 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_92 double precision,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision,
    f_162 double precision
);


--
-- Name: prioritizr_input_run_82; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_82 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    locked_in integer,
    locked_out integer,
    f_92 double precision,
    f_100 double precision,
    f_101 double precision,
    f_103 double precision,
    f_105 double precision,
    f_107 double precision,
    f_109 double precision,
    f_110 double precision,
    f_111 double precision,
    f_112 double precision,
    f_113 double precision,
    f_115 double precision,
    f_116 double precision,
    f_117 double precision,
    f_118 double precision,
    f_119 double precision,
    f_120 double precision,
    f_121 double precision,
    f_122 double precision,
    f_123 double precision,
    f_124 double precision,
    f_128 double precision,
    f_129 double precision,
    f_130 double precision,
    f_132 double precision,
    f_133 double precision,
    f_162 double precision,
    f_163 double precision
);


--
-- Name: prioritizr_input_run_9; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE UNLOGGED TABLE bioprotect.prioritizr_input_run_9 (
    pu_id text,
    geometry public.geometry(Polygon,4326),
    cost double precision,
    area_km2 double precision,
    f_19 double precision,
    f_21 double precision,
    f_22 double precision,
    f_23 double precision,
    f_24 double precision,
    f_25 double precision,
    f_26 double precision,
    f_27 double precision,
    f_28 double precision,
    f_29 double precision,
    f_30 double precision,
    f_31 double precision,
    f_32 double precision,
    f_33 double precision,
    f_34 double precision,
    f_35 double precision,
    f_36 double precision,
    f_37 double precision,
    f_71 double precision
);


--
-- Name: prioritizr_run_logs; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.prioritizr_run_logs (
    id bigint NOT NULL,
    run_id bigint NOT NULL,
    ts timestamp with time zone DEFAULT now() NOT NULL,
    stream text NOT NULL,
    message text NOT NULL
);


--
-- Name: prioritizr_run_logs_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.prioritizr_run_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: prioritizr_run_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.prioritizr_run_logs_id_seq OWNED BY bioprotect.prioritizr_run_logs.id;


--
-- Name: prioritizr_run_results; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.prioritizr_run_results (
    run_id bigint NOT NULL,
    h3_index text NOT NULL,
    solution integer NOT NULL
);


--
-- Name: prioritizr_runs; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.prioritizr_runs (
    id bigint NOT NULL,
    project_id integer NOT NULL,
    created_by integer,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    status text DEFAULT 'queued'::text NOT NULL,
    params jsonb DEFAULT '{}'::jsonb NOT NULL,
    input_table text,
    feature_cols text[],
    error text,
    label text,
    resolved_config jsonb,
    feature_map jsonb,
    description text
);


--
-- Name: prioritizr_runs_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.prioritizr_runs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: prioritizr_runs_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.prioritizr_runs_id_seq OWNED BY bioprotect.prioritizr_runs.id;


--
-- Name: project_features; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.project_features (
    project_id bigint NOT NULL,
    feature_unique_id bigint NOT NULL,
    target_type text DEFAULT 'prop'::text,
    target_value numeric,
    spf numeric DEFAULT 40,
    weight numeric,
    updated_at timestamp with time zone DEFAULT now(),
    created_at timestamp with time zone DEFAULT now(),
    CONSTRAINT project_features_target_type_check CHECK ((target_type = ANY (ARRAY['prop'::text, 'abs'::text])))
);


--
-- Name: project_files; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.project_files (
    id integer NOT NULL,
    project_id integer,
    file_type text NOT NULL,
    file_name text
);


--
-- Name: project_files_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.project_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: project_files_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.project_files_id_seq OWNED BY bioprotect.project_files.id;


--
-- Name: project_metadata; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.project_metadata (
    id integer NOT NULL,
    project_id integer,
    key text,
    value text
);


--
-- Name: project_metadata_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.project_metadata_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: project_metadata_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.project_metadata_id_seq OWNED BY bioprotect.project_metadata.id;


--
-- Name: project_pus; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.project_pus (
    id integer NOT NULL,
    project_id integer,
    h3_index text
);


--
-- Name: project_pus_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.project_pus_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: project_pus_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.project_pus_id_seq OWNED BY bioprotect.project_pus.id;


--
-- Name: project_renderer; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.project_renderer (
    id integer NOT NULL,
    project_id integer,
    key text NOT NULL,
    value text
);


--
-- Name: project_renderer_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.project_renderer_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: project_renderer_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.project_renderer_id_seq OWNED BY bioprotect.project_renderer.id;


--
-- Name: project_run_parameters; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.project_run_parameters (
    id integer NOT NULL,
    project_id integer,
    key text NOT NULL,
    value text
);


--
-- Name: project_run_parameters_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.project_run_parameters_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: project_run_parameters_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.project_run_parameters_id_seq OWNED BY bioprotect.project_run_parameters.id;


--
-- Name: projects; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.projects (
    id integer NOT NULL,
    name text NOT NULL,
    description text,
    date_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    planning_unit_id integer,
    old_version boolean,
    iucn_category text,
    is_private boolean,
    costs text,
    default_resolution integer DEFAULT 7,
    active_cost_profile_id integer
);


--
-- Name: projects_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.projects_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: projects_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.projects_id_seq OWNED BY bioprotect.projects.id;


--
-- Name: pu_feature_amounts; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.pu_feature_amounts (
    project_id integer NOT NULL,
    feature_unique_id integer NOT NULL,
    h3_index text NOT NULL,
    amount double precision NOT NULL,
    CONSTRAINT pu_feature_amounts_amount_check CHECK ((amount >= (0)::double precision))
);


--
-- Name: pu_feature_amounts_backup_km2migration; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.pu_feature_amounts_backup_km2migration (
    project_id integer,
    feature_unique_id integer,
    h3_index text,
    amount double precision
);


--
-- Name: pu_h3; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.pu_h3 (
    pu_id integer NOT NULL,
    h3_index text NOT NULL,
    resolution integer NOT NULL
);


--
-- Name: res9_costs_staging; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.res9_costs_staging (
    h3_index text NOT NULL,
    hii_scaled numeric
);


--
-- Name: schema_migrations; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.schema_migrations (
    version text NOT NULL,
    name text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: sensitivity_matrix; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.sensitivity_matrix (
    id integer NOT NULL,
    eunis_code text NOT NULL,
    pressure text NOT NULL,
    sensitivity_score numeric NOT NULL
);


--
-- Name: sensitivity_matrix_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.sensitivity_matrix_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: sensitivity_matrix_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.sensitivity_matrix_id_seq OWNED BY bioprotect.sensitivity_matrix.id;


--
-- Name: species_data; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.species_data (
    id integer NOT NULL,
    project_id integer NOT NULL,
    feature_unique_id integer NOT NULL,
    prop numeric NOT NULL,
    spf integer NOT NULL
);


--
-- Name: species_data_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.species_data_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: species_data_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.species_data_id_seq OWNED BY bioprotect.species_data.id;


--
-- Name: temp_pressures_rid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.temp_pressures_rid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: test_rid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.test_rid_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: user_projects; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.user_projects (
    user_id integer NOT NULL,
    project_id integer NOT NULL,
    role text NOT NULL,
    added_at timestamp with time zone DEFAULT now(),
    CONSTRAINT user_projects_role_check CHECK ((role = ANY (ARRAY['owner'::text, 'editor'::text, 'viewer'::text])))
);


--
-- Name: users; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.users (
    id integer NOT NULL,
    username character varying(50) NOT NULL,
    password_hash character varying(255) NOT NULL,
    last_project integer,
    show_popup boolean DEFAULT false,
    email text,
    basemap text,
    role text NOT NULL,
    date_created timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
    use_feature_colours boolean DEFAULT false,
    report_units text,
    refresh_tokens text[] DEFAULT '{}'::text[]
);


--
-- Name: users_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.users_id_seq OWNED BY bioprotect.users.id;


--
-- Name: v_h3_adriatic_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_adriatic_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Adriatic Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_aegean_levantine_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_aegean_levantine_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Aegean-Levantine Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_arctic_ocean_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_arctic_ocean_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Arctic Ocean'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_azores_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_azores_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Azores'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_baltic_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_baltic_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Baltic Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_barents_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_barents_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Barents Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_bay_of_biscay_and_the_iberian_coast_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_bay_of_biscay_and_the_iberian_coast_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Bay of Biscay and the Iberian Coast'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_black_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_black_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Black Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_case_study_extents_water_only_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_case_study_extents_water_only_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    (public.st_setsrid(h3_cells.geometry, 4326))::public.geometry(Polygon,4326) AS geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Case Study Extents Water Only'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_case_study_extents_water_only_res9; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_case_study_extents_water_only_res9 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    (public.st_setsrid(h3_cells.geometry, 4326))::public.geometry(Polygon,4326) AS geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Case Study Extents Water Only'::text) AND (h3_cells.resolution = 9))
  WITH NO DATA;


--
-- Name: v_h3_celtic_seas_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_celtic_seas_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Celtic Seas'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_faroes_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_faroes_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Faroes'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_greater_north_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_greater_north_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Greater North Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_greenland_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_greenland_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Greenland Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_icelandic_waters_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_icelandic_waters_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Icelandic Waters'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_ionian_sea_and_the_central_mediterranean_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_ionian_sea_and_the_central_mediterranean_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Ionian Sea and the Central Mediterranean Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_msp_assessment_area_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_msp_assessment_area_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'MSP Assessment Area'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_norwegian_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_norwegian_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Norwegian Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_oceanic_northeast_atlantic_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_oceanic_northeast_atlantic_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Oceanic Northeast Atlantic'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: v_h3_western_mediterranean_sea_res7; Type: MATERIALIZED VIEW; Schema: bioprotect; Owner: -
--

CREATE MATERIALIZED VIEW bioprotect.v_h3_western_mediterranean_sea_res7 AS
 SELECT h3_cells.h3_index,
    h3_cells.resolution,
    h3_cells.scale_level,
    h3_cells.project_area,
    h3_cells.geometry
   FROM bioprotect.h3_cells
  WHERE ((h3_cells.project_area = 'Western Mediterranean Sea'::text) AND (h3_cells.resolution = 7))
  WITH NO DATA;


--
-- Name: wdpa; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.wdpa (
    wdpaid double precision,
    iucn_cat character varying,
    shape_leng double precision,
    shape_area double precision,
    geometry public.geometry(Geometry,4326),
    iso3 character varying(80),
    status text,
    desig text
);


--
-- Name: wdpa_272_dissolved; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.wdpa_272_dissolved (
    geom public.geometry
);


--
-- Name: activity_0011a2c9745b489cb90400b id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_0011a2c9745b489cb90400b ALTER COLUMN id SET DEFAULT nextval('bioprotect.activity_0011a2c9745b489cb90400b_id_seq'::regclass);


--
-- Name: activity_2712fe1f88d942ac9cd34a5 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_2712fe1f88d942ac9cd34a5 ALTER COLUMN id SET DEFAULT nextval('bioprotect.activity_2712fe1f88d942ac9cd34a5_id_seq'::regclass);


--
-- Name: activity_37910b3f692d41959faa618 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_37910b3f692d41959faa618 ALTER COLUMN id SET DEFAULT nextval('bioprotect.activity_37910b3f692d41959faa618_id_seq'::regclass);


--
-- Name: activity_d45578c468bd4f12a91bd67 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_d45578c468bd4f12a91bd67 ALTER COLUMN id SET DEFAULT nextval('bioprotect.activity_d45578c468bd4f12a91bd67_id_seq'::regclass);


--
-- Name: activity_f939f8d4bee04500af1e172 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_f939f8d4bee04500af1e172 ALTER COLUMN id SET DEFAULT nextval('bioprotect.activity_f939f8d4bee04500af1e172_id_seq'::regclass);


--
-- Name: cost_profile_values id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_values ALTER COLUMN id SET DEFAULT nextval('bioprotect.cost_profile_values_id_seq'::regclass);


--
-- Name: cost_profiles id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profiles ALTER COLUMN id SET DEFAULT nextval('bioprotect.cost_profiles_id_seq'::regclass);


--
-- Name: eez_simplified_1km ogc_fid; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.eez_simplified_1km ALTER COLUMN ogc_fid SET DEFAULT nextval('bioprotect.eez_simplified_1km_ogc_fid_seq'::regclass);


--
-- Name: f_01889e7f71624c0d9f2ad2c3241708 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_01889e7f71624c0d9f2ad2c3241708 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_01889e7f71624c0d9f2ad2c3241708_id_seq'::regclass);


--
-- Name: f_0b85bb11b41a4e269c0aebfbe0b544 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544_id_seq'::regclass);


--
-- Name: f_0c897459e69d4c22a1c00299aa5547 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_0c897459e69d4c22a1c00299aa5547 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_0c897459e69d4c22a1c00299aa5547_id_seq'::regclass);


--
-- Name: f_148a329e7dba44e8aea3c1151422ce id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_148a329e7dba44e8aea3c1151422ce ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_148a329e7dba44e8aea3c1151422ce_id_seq'::regclass);


--
-- Name: f_187731fcf47b4d2e982a38b5532a3a id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_187731fcf47b4d2e982a38b5532a3a ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_187731fcf47b4d2e982a38b5532a3a_id_seq'::regclass);


--
-- Name: f_1ca7a0ceec044faa9e153e72fce960 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_1ca7a0ceec044faa9e153e72fce960 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_1ca7a0ceec044faa9e153e72fce960_id_seq'::regclass);


--
-- Name: f_1f528ee9c1ee4a37a2e1ebbb1af2a9 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9_id_seq'::regclass);


--
-- Name: f_2187b5a975fe402e8f9a17cfa55eac id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2187b5a975fe402e8f9a17cfa55eac ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_2187b5a975fe402e8f9a17cfa55eac_id_seq'::regclass);


--
-- Name: f_21debeb092f844f2b64b001bd64c29 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_21debeb092f844f2b64b001bd64c29 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_21debeb092f844f2b64b001bd64c29_id_seq'::regclass);


--
-- Name: f_234e530d24fc4252bed2b3e84ee1ba id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_234e530d24fc4252bed2b3e84ee1ba ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_234e530d24fc4252bed2b3e84ee1ba_id_seq'::regclass);


--
-- Name: f_245a2235f8d642269a1aaa82bedcb5 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_245a2235f8d642269a1aaa82bedcb5 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_245a2235f8d642269a1aaa82bedcb5_id_seq'::regclass);


--
-- Name: f_26a73e4c6eb0425e8c998ce0cb84b4 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4_id_seq'::regclass);


--
-- Name: f_2a587cadbd434a72b87ff6a2c3cc77 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77_id_seq'::regclass);


--
-- Name: f_2ccd7d878404486c8b8b9d5c90e9fc id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc_id_seq'::regclass);


--
-- Name: f_2ec291545abc47aea06b69f21e192c id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2ec291545abc47aea06b69f21e192c ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_2ec291545abc47aea06b69f21e192c_id_seq'::regclass);


--
-- Name: f_2f5e67d4c1274b1eaf9a5a626a4282 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282_id_seq'::regclass);


--
-- Name: f_3061a71a8f0244848fbbff758c198e id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_3061a71a8f0244848fbbff758c198e ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_3061a71a8f0244848fbbff758c198e_id_seq'::regclass);


--
-- Name: f_352d7d971b674f06bcc2eb894c8685 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_352d7d971b674f06bcc2eb894c8685 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_352d7d971b674f06bcc2eb894c8685_id_seq'::regclass);


--
-- Name: f_37a68a8282d3495daee95f63fb8f6f id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_37a68a8282d3495daee95f63fb8f6f ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_37a68a8282d3495daee95f63fb8f6f_id_seq'::regclass);


--
-- Name: f_3923082fbb6e4b0bb22fbca0530c2f id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f_id_seq'::regclass);


--
-- Name: f_3bcc07d1d7e142ddbe995cee1c2060 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060_id_seq'::regclass);


--
-- Name: f_459758257ec542739ee8c64554920e id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_459758257ec542739ee8c64554920e ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_459758257ec542739ee8c64554920e_id_seq'::regclass);


--
-- Name: f_4dcb01bda5214973ae6d3d2f02982f id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_4dcb01bda5214973ae6d3d2f02982f ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_4dcb01bda5214973ae6d3d2f02982f_id_seq'::regclass);


--
-- Name: f_52d03f922eb14788ae1bef30f1429e id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_52d03f922eb14788ae1bef30f1429e ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_52d03f922eb14788ae1bef30f1429e_id_seq'::regclass);


--
-- Name: f_65f429eb64904e2a8c4d4f55a13cc9 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9_id_seq'::regclass);


--
-- Name: f_6deb40e7592e45f79fe1eb99a5f590 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_6deb40e7592e45f79fe1eb99a5f590 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_6deb40e7592e45f79fe1eb99a5f590_id_seq'::regclass);


--
-- Name: f_717bb7ba09ca4e38a18bdd04a88cc2 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2_id_seq'::regclass);


--
-- Name: f_7a5e4445f2a248d798a1e1a2b3d8c1 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1_id_seq'::regclass);


--
-- Name: f_7b6705b767494f6cb8c937e4929fd4 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7b6705b767494f6cb8c937e4929fd4 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_7b6705b767494f6cb8c937e4929fd4_id_seq'::regclass);


--
-- Name: f_7c1b92efed5443b78dd4d0d09121c9 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7c1b92efed5443b78dd4d0d09121c9 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_7c1b92efed5443b78dd4d0d09121c9_id_seq'::regclass);


--
-- Name: f_7eb4617384134a47982d7eee19769d id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7eb4617384134a47982d7eee19769d ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_7eb4617384134a47982d7eee19769d_id_seq'::regclass);


--
-- Name: f_842802b2046c420b87a3d131633526 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_842802b2046c420b87a3d131633526 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_842802b2046c420b87a3d131633526_id_seq'::regclass);


--
-- Name: f_88a663104e2e4eb58f77c06d2c2480 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_88a663104e2e4eb58f77c06d2c2480 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_88a663104e2e4eb58f77c06d2c2480_id_seq'::regclass);


--
-- Name: f_8e49e2ec060746578e1fec042d6565 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_8e49e2ec060746578e1fec042d6565 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_8e49e2ec060746578e1fec042d6565_id_seq'::regclass);


--
-- Name: f_9081d33eaa78434bbee51ed915a94c id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_9081d33eaa78434bbee51ed915a94c ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_9081d33eaa78434bbee51ed915a94c_id_seq'::regclass);


--
-- Name: f_91ecb37cc23e4c8a86fa08c9902d80 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80_id_seq'::regclass);


--
-- Name: f_99e1849671054f0eb34effaefe2064 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_99e1849671054f0eb34effaefe2064 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_99e1849671054f0eb34effaefe2064_id_seq'::regclass);


--
-- Name: f_a24524f50a444a4689645403db2ef8 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a24524f50a444a4689645403db2ef8 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_a24524f50a444a4689645403db2ef8_id_seq'::regclass);


--
-- Name: f_a3da4861d02a4adba10e55b9ab9e6e id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e_id_seq'::regclass);


--
-- Name: f_a5e612cd6d394f28802adebcac250a id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a5e612cd6d394f28802adebcac250a ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_a5e612cd6d394f28802adebcac250a_id_seq'::regclass);


--
-- Name: f_a7d1c02731ec419082a7277fe13cc0 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a7d1c02731ec419082a7277fe13cc0 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_a7d1c02731ec419082a7277fe13cc0_id_seq'::regclass);


--
-- Name: f_aef913e8ecd147299348eb5e9a629f id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_aef913e8ecd147299348eb5e9a629f ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_aef913e8ecd147299348eb5e9a629f_id_seq'::regclass);


--
-- Name: f_b1d980c8b69441dc9ff24b14237f11 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b1d980c8b69441dc9ff24b14237f11 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b1d980c8b69441dc9ff24b14237f11_id_seq'::regclass);


--
-- Name: f_b25a913184af4c39bd069076ccee9c id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b25a913184af4c39bd069076ccee9c ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b25a913184af4c39bd069076ccee9c_id_seq'::regclass);


--
-- Name: f_b3491f0025d34f0194be96e4547e36 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b3491f0025d34f0194be96e4547e36 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b3491f0025d34f0194be96e4547e36_id_seq'::regclass);


--
-- Name: f_b3d1f9032ad243d18e768a8d7e3f76 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76_id_seq'::regclass);


--
-- Name: f_b4d2b4619455408e9ba0f02386e539 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b4d2b4619455408e9ba0f02386e539 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b4d2b4619455408e9ba0f02386e539_id_seq'::regclass);


--
-- Name: f_b4e74e66d4bf4a428824a10431e0d7 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b4e74e66d4bf4a428824a10431e0d7 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b4e74e66d4bf4a428824a10431e0d7_id_seq'::regclass);


--
-- Name: f_b7052d76f72148b9aecbf08f7d300f id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b7052d76f72148b9aecbf08f7d300f ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b7052d76f72148b9aecbf08f7d300f_id_seq'::regclass);


--
-- Name: f_b7dc2be4209a496aa66e85f457a443 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b7dc2be4209a496aa66e85f457a443 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b7dc2be4209a496aa66e85f457a443_id_seq'::regclass);


--
-- Name: f_b9d3d3026af049c2a22e3bb79a3869 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b9d3d3026af049c2a22e3bb79a3869 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b9d3d3026af049c2a22e3bb79a3869_id_seq'::regclass);


--
-- Name: f_b9f108f222074be5b2d7bfac0705b3 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b9f108f222074be5b2d7bfac0705b3 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_b9f108f222074be5b2d7bfac0705b3_id_seq'::regclass);


--
-- Name: f_bd44b7714c7f4dc2be4ab9c23e44c3 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3_id_seq'::regclass);


--
-- Name: f_bf7c637287f149348dbe268887459a id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_bf7c637287f149348dbe268887459a ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_bf7c637287f149348dbe268887459a_id_seq'::regclass);


--
-- Name: f_c00802c241d6413b8b1bc15677a816 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_c00802c241d6413b8b1bc15677a816 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_c00802c241d6413b8b1bc15677a816_id_seq'::regclass);


--
-- Name: f_c275409be23c4ffcae31cf9346077e id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_c275409be23c4ffcae31cf9346077e ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_c275409be23c4ffcae31cf9346077e_id_seq'::regclass);


--
-- Name: f_c6c00aba44cd4932a704eb64605d30 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_c6c00aba44cd4932a704eb64605d30 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_c6c00aba44cd4932a704eb64605d30_id_seq'::regclass);


--
-- Name: f_cb5c84470af44206942d56e73f2537 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cb5c84470af44206942d56e73f2537 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_cb5c84470af44206942d56e73f2537_id_seq'::regclass);


--
-- Name: f_cb7eed03815440278883a83e21d29a id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cb7eed03815440278883a83e21d29a ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_cb7eed03815440278883a83e21d29a_id_seq'::regclass);


--
-- Name: f_cd23e938324b47699975c875199bc9 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cd23e938324b47699975c875199bc9 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_cd23e938324b47699975c875199bc9_id_seq'::regclass);


--
-- Name: f_cef1ce78dbf649c2a5b936227217db id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cef1ce78dbf649c2a5b936227217db ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_cef1ce78dbf649c2a5b936227217db_id_seq'::regclass);


--
-- Name: f_dd21a32dab4d4971851c4ed1f7aae9 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9_id_seq'::regclass);


--
-- Name: f_ddc3ac5aa4c54101bf2ac019495a37 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37_id_seq'::regclass);


--
-- Name: f_e65e35e598e243419bcb96e759b2eb id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_e65e35e598e243419bcb96e759b2eb ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_e65e35e598e243419bcb96e759b2eb_id_seq'::regclass);


--
-- Name: f_e93241238a0f4153a9c8d648e55662 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_e93241238a0f4153a9c8d648e55662 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_e93241238a0f4153a9c8d648e55662_id_seq'::regclass);


--
-- Name: f_ea9d7de99bb2447986b4b4849eeb81 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_ea9d7de99bb2447986b4b4849eeb81 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_ea9d7de99bb2447986b4b4849eeb81_id_seq'::regclass);


--
-- Name: f_f22a1a11a1134a86ab6e32ad52ccf3 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3_id_seq'::regclass);


--
-- Name: f_f2b98a9b43ac45778f17d27444ad6d id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f2b98a9b43ac45778f17d27444ad6d ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_f2b98a9b43ac45778f17d27444ad6d_id_seq'::regclass);


--
-- Name: f_f5431315d3d149d2a707d5c829b703 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f5431315d3d149d2a707d5c829b703 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_f5431315d3d149d2a707d5c829b703_id_seq'::regclass);


--
-- Name: f_f782eda898d94749947dd8ba1ced20 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f782eda898d94749947dd8ba1ced20 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_f782eda898d94749947dd8ba1ced20_id_seq'::regclass);


--
-- Name: f_f8dfa6923e2d46a5afb5cb3c778894 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894_id_seq'::regclass);


--
-- Name: f_f9e0fe869ca14d4ebf8cf503df4ea5 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5_id_seq'::regclass);


--
-- Name: f_fb1438f3390a438d96a21b870e3319 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_fb1438f3390a438d96a21b870e3319 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_fb1438f3390a438d96a21b870e3319_id_seq'::regclass);


--
-- Name: f_fba08cfc6e534e778af5888e6cabd6 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_fba08cfc6e534e778af5888e6cabd6 ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_fba08cfc6e534e778af5888e6cabd6_id_seq'::regclass);


--
-- Name: f_fd18eeb663824a85aa6b05987c781c id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_fd18eeb663824a85aa6b05987c781c ALTER COLUMN id SET DEFAULT nextval('bioprotect.f_fd18eeb663824a85aa6b05987c781c_id_seq'::regclass);


--
-- Name: gaul_2015_simplified_1km ogc_fid; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.gaul_2015_simplified_1km ALTER COLUMN ogc_fid SET DEFAULT nextval('bioprotect.gaul_2015_simplified_1km_ogc_fid_seq'::regclass);


--
-- Name: gbif_2480623 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.gbif_2480623 ALTER COLUMN id SET DEFAULT nextval('bioprotect.gbif_2480623_id_seq'::regclass);


--
-- Name: gbif_2486629 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.gbif_2486629 ALTER COLUMN id SET DEFAULT nextval('bioprotect.gbif_2486629_id_seq'::regclass);


--
-- Name: gbif_2486630 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.gbif_2486630 ALTER COLUMN id SET DEFAULT nextval('bioprotect.gbif_2486630_id_seq'::regclass);


--
-- Name: gbif_2495255 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.gbif_2495255 ALTER COLUMN id SET DEFAULT nextval('bioprotect.gbif_2495255_id_seq'::regclass);


--
-- Name: gbif_5230455 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.gbif_5230455 ALTER COLUMN id SET DEFAULT nextval('bioprotect.gbif_5230455_id_seq'::regclass);


--
-- Name: gbif_9056437 id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.gbif_9056437 ALTER COLUMN id SET DEFAULT nextval('bioprotect.gbif_9056437_id_seq'::regclass);


--
-- Name: ices_ecoregions ogc_fid; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.ices_ecoregions ALTER COLUMN ogc_fid SET DEFAULT nextval('bioprotect.ices_ecoregions_ogc_fid_seq'::regclass);


--
-- Name: jncc_sensitivities id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.jncc_sensitivities ALTER COLUMN id SET DEFAULT nextval('bioprotect.jncc_sensitivities_id_seq'::regclass);


--
-- Name: metadata_activities id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_activities ALTER COLUMN id SET DEFAULT nextval('bioprotect.metadata_activities_id_seq'::regclass);


--
-- Name: metadata_impacts id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_impacts ALTER COLUMN id SET DEFAULT nextval('bioprotect.metadata_impacts_id_seq'::regclass);


--
-- Name: metadata_interest_features unique_id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_interest_features ALTER COLUMN unique_id SET DEFAULT nextval('bioprotect.metadata_interest_features_unique_id_seq'::regclass);


--
-- Name: metadata_planning_units unique_id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_planning_units ALTER COLUMN unique_id SET DEFAULT nextval('bioprotect.metadata_planning_units_unique_id_seq'::regclass);


--
-- Name: pressures id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pressures ALTER COLUMN id SET DEFAULT nextval('bioprotect.pressures_id_seq'::regclass);


--
-- Name: prioritizr_run_logs id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_run_logs ALTER COLUMN id SET DEFAULT nextval('bioprotect.prioritizr_run_logs_id_seq'::regclass);


--
-- Name: prioritizr_runs id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_runs ALTER COLUMN id SET DEFAULT nextval('bioprotect.prioritizr_runs_id_seq'::regclass);


--
-- Name: project_files id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_files ALTER COLUMN id SET DEFAULT nextval('bioprotect.project_files_id_seq'::regclass);


--
-- Name: project_metadata id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_metadata ALTER COLUMN id SET DEFAULT nextval('bioprotect.project_metadata_id_seq'::regclass);


--
-- Name: project_pus id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_pus ALTER COLUMN id SET DEFAULT nextval('bioprotect.project_pus_id_seq'::regclass);


--
-- Name: project_renderer id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_renderer ALTER COLUMN id SET DEFAULT nextval('bioprotect.project_renderer_id_seq'::regclass);


--
-- Name: project_run_parameters id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_run_parameters ALTER COLUMN id SET DEFAULT nextval('bioprotect.project_run_parameters_id_seq'::regclass);


--
-- Name: projects id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.projects ALTER COLUMN id SET DEFAULT nextval('bioprotect.projects_id_seq'::regclass);


--
-- Name: sensitivity_matrix id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.sensitivity_matrix ALTER COLUMN id SET DEFAULT nextval('bioprotect.sensitivity_matrix_id_seq'::regclass);


--
-- Name: species_data id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.species_data ALTER COLUMN id SET DEFAULT nextval('bioprotect.species_data_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.users ALTER COLUMN id SET DEFAULT nextval('bioprotect.users_id_seq'::regclass);


--
-- Name: activity_0011a2c9745b489cb90400b activity_0011a2c9745b489cb90400b_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_0011a2c9745b489cb90400b
    ADD CONSTRAINT activity_0011a2c9745b489cb90400b_pkey PRIMARY KEY (id);


--
-- Name: activity_2712fe1f88d942ac9cd34a5 activity_2712fe1f88d942ac9cd34a5_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_2712fe1f88d942ac9cd34a5
    ADD CONSTRAINT activity_2712fe1f88d942ac9cd34a5_pkey PRIMARY KEY (id);


--
-- Name: activity_37910b3f692d41959faa618 activity_37910b3f692d41959faa618_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_37910b3f692d41959faa618
    ADD CONSTRAINT activity_37910b3f692d41959faa618_pkey PRIMARY KEY (id);


--
-- Name: activity_d45578c468bd4f12a91bd67 activity_d45578c468bd4f12a91bd67_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_d45578c468bd4f12a91bd67
    ADD CONSTRAINT activity_d45578c468bd4f12a91bd67_pkey PRIMARY KEY (id);


--
-- Name: activity_f939f8d4bee04500af1e172 activity_f939f8d4bee04500af1e172_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.activity_f939f8d4bee04500af1e172
    ADD CONSTRAINT activity_f939f8d4bee04500af1e172_pkey PRIMARY KEY (id);


--
-- Name: cost_profile_values cost_profile_values_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_values
    ADD CONSTRAINT cost_profile_values_pkey PRIMARY KEY (id);


--
-- Name: cost_profiles cost_profiles_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profiles
    ADD CONSTRAINT cost_profiles_pkey PRIMARY KEY (id);


--
-- Name: f_01889e7f71624c0d9f2ad2c3241708 f_01889e7f71624c0d9f2ad2c3241708_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_01889e7f71624c0d9f2ad2c3241708
    ADD CONSTRAINT f_01889e7f71624c0d9f2ad2c3241708_pkey PRIMARY KEY (id);


--
-- Name: f_0b85bb11b41a4e269c0aebfbe0b544 f_0b85bb11b41a4e269c0aebfbe0b544_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544
    ADD CONSTRAINT f_0b85bb11b41a4e269c0aebfbe0b544_pkey PRIMARY KEY (id);


--
-- Name: f_0c897459e69d4c22a1c00299aa5547 f_0c897459e69d4c22a1c00299aa5547_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_0c897459e69d4c22a1c00299aa5547
    ADD CONSTRAINT f_0c897459e69d4c22a1c00299aa5547_pkey PRIMARY KEY (id);


--
-- Name: f_148a329e7dba44e8aea3c1151422ce f_148a329e7dba44e8aea3c1151422ce_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_148a329e7dba44e8aea3c1151422ce
    ADD CONSTRAINT f_148a329e7dba44e8aea3c1151422ce_pkey PRIMARY KEY (id);


--
-- Name: f_187731fcf47b4d2e982a38b5532a3a f_187731fcf47b4d2e982a38b5532a3a_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_187731fcf47b4d2e982a38b5532a3a
    ADD CONSTRAINT f_187731fcf47b4d2e982a38b5532a3a_pkey PRIMARY KEY (id);


--
-- Name: f_1ca7a0ceec044faa9e153e72fce960 f_1ca7a0ceec044faa9e153e72fce960_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_1ca7a0ceec044faa9e153e72fce960
    ADD CONSTRAINT f_1ca7a0ceec044faa9e153e72fce960_pkey PRIMARY KEY (id);


--
-- Name: f_1f528ee9c1ee4a37a2e1ebbb1af2a9 f_1f528ee9c1ee4a37a2e1ebbb1af2a9_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9
    ADD CONSTRAINT f_1f528ee9c1ee4a37a2e1ebbb1af2a9_pkey PRIMARY KEY (id);


--
-- Name: f_2187b5a975fe402e8f9a17cfa55eac f_2187b5a975fe402e8f9a17cfa55eac_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2187b5a975fe402e8f9a17cfa55eac
    ADD CONSTRAINT f_2187b5a975fe402e8f9a17cfa55eac_pkey PRIMARY KEY (id);


--
-- Name: f_21debeb092f844f2b64b001bd64c29 f_21debeb092f844f2b64b001bd64c29_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_21debeb092f844f2b64b001bd64c29
    ADD CONSTRAINT f_21debeb092f844f2b64b001bd64c29_pkey PRIMARY KEY (id);


--
-- Name: f_234e530d24fc4252bed2b3e84ee1ba f_234e530d24fc4252bed2b3e84ee1ba_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_234e530d24fc4252bed2b3e84ee1ba
    ADD CONSTRAINT f_234e530d24fc4252bed2b3e84ee1ba_pkey PRIMARY KEY (id);


--
-- Name: f_245a2235f8d642269a1aaa82bedcb5 f_245a2235f8d642269a1aaa82bedcb5_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_245a2235f8d642269a1aaa82bedcb5
    ADD CONSTRAINT f_245a2235f8d642269a1aaa82bedcb5_pkey PRIMARY KEY (id);


--
-- Name: f_26a73e4c6eb0425e8c998ce0cb84b4 f_26a73e4c6eb0425e8c998ce0cb84b4_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4
    ADD CONSTRAINT f_26a73e4c6eb0425e8c998ce0cb84b4_pkey PRIMARY KEY (id);


--
-- Name: f_2a587cadbd434a72b87ff6a2c3cc77 f_2a587cadbd434a72b87ff6a2c3cc77_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77
    ADD CONSTRAINT f_2a587cadbd434a72b87ff6a2c3cc77_pkey PRIMARY KEY (id);


--
-- Name: f_2ccd7d878404486c8b8b9d5c90e9fc f_2ccd7d878404486c8b8b9d5c90e9fc_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc
    ADD CONSTRAINT f_2ccd7d878404486c8b8b9d5c90e9fc_pkey PRIMARY KEY (id);


--
-- Name: f_2ec291545abc47aea06b69f21e192c f_2ec291545abc47aea06b69f21e192c_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2ec291545abc47aea06b69f21e192c
    ADD CONSTRAINT f_2ec291545abc47aea06b69f21e192c_pkey PRIMARY KEY (id);


--
-- Name: f_2f5e67d4c1274b1eaf9a5a626a4282 f_2f5e67d4c1274b1eaf9a5a626a4282_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282
    ADD CONSTRAINT f_2f5e67d4c1274b1eaf9a5a626a4282_pkey PRIMARY KEY (id);


--
-- Name: f_3061a71a8f0244848fbbff758c198e f_3061a71a8f0244848fbbff758c198e_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_3061a71a8f0244848fbbff758c198e
    ADD CONSTRAINT f_3061a71a8f0244848fbbff758c198e_pkey PRIMARY KEY (id);


--
-- Name: f_352d7d971b674f06bcc2eb894c8685 f_352d7d971b674f06bcc2eb894c8685_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_352d7d971b674f06bcc2eb894c8685
    ADD CONSTRAINT f_352d7d971b674f06bcc2eb894c8685_pkey PRIMARY KEY (id);


--
-- Name: f_37a68a8282d3495daee95f63fb8f6f f_37a68a8282d3495daee95f63fb8f6f_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_37a68a8282d3495daee95f63fb8f6f
    ADD CONSTRAINT f_37a68a8282d3495daee95f63fb8f6f_pkey PRIMARY KEY (id);


--
-- Name: f_3923082fbb6e4b0bb22fbca0530c2f f_3923082fbb6e4b0bb22fbca0530c2f_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f
    ADD CONSTRAINT f_3923082fbb6e4b0bb22fbca0530c2f_pkey PRIMARY KEY (id);


--
-- Name: f_3bcc07d1d7e142ddbe995cee1c2060 f_3bcc07d1d7e142ddbe995cee1c2060_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060
    ADD CONSTRAINT f_3bcc07d1d7e142ddbe995cee1c2060_pkey PRIMARY KEY (id);


--
-- Name: f_459758257ec542739ee8c64554920e f_459758257ec542739ee8c64554920e_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_459758257ec542739ee8c64554920e
    ADD CONSTRAINT f_459758257ec542739ee8c64554920e_pkey PRIMARY KEY (id);


--
-- Name: f_4dcb01bda5214973ae6d3d2f02982f f_4dcb01bda5214973ae6d3d2f02982f_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_4dcb01bda5214973ae6d3d2f02982f
    ADD CONSTRAINT f_4dcb01bda5214973ae6d3d2f02982f_pkey PRIMARY KEY (id);


--
-- Name: f_52d03f922eb14788ae1bef30f1429e f_52d03f922eb14788ae1bef30f1429e_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_52d03f922eb14788ae1bef30f1429e
    ADD CONSTRAINT f_52d03f922eb14788ae1bef30f1429e_pkey PRIMARY KEY (id);


--
-- Name: f_65f429eb64904e2a8c4d4f55a13cc9 f_65f429eb64904e2a8c4d4f55a13cc9_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9
    ADD CONSTRAINT f_65f429eb64904e2a8c4d4f55a13cc9_pkey PRIMARY KEY (id);


--
-- Name: f_6deb40e7592e45f79fe1eb99a5f590 f_6deb40e7592e45f79fe1eb99a5f590_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_6deb40e7592e45f79fe1eb99a5f590
    ADD CONSTRAINT f_6deb40e7592e45f79fe1eb99a5f590_pkey PRIMARY KEY (id);


--
-- Name: f_717bb7ba09ca4e38a18bdd04a88cc2 f_717bb7ba09ca4e38a18bdd04a88cc2_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2
    ADD CONSTRAINT f_717bb7ba09ca4e38a18bdd04a88cc2_pkey PRIMARY KEY (id);


--
-- Name: f_7a5e4445f2a248d798a1e1a2b3d8c1 f_7a5e4445f2a248d798a1e1a2b3d8c1_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1
    ADD CONSTRAINT f_7a5e4445f2a248d798a1e1a2b3d8c1_pkey PRIMARY KEY (id);


--
-- Name: f_7b6705b767494f6cb8c937e4929fd4 f_7b6705b767494f6cb8c937e4929fd4_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7b6705b767494f6cb8c937e4929fd4
    ADD CONSTRAINT f_7b6705b767494f6cb8c937e4929fd4_pkey PRIMARY KEY (id);


--
-- Name: f_7c1b92efed5443b78dd4d0d09121c9 f_7c1b92efed5443b78dd4d0d09121c9_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7c1b92efed5443b78dd4d0d09121c9
    ADD CONSTRAINT f_7c1b92efed5443b78dd4d0d09121c9_pkey PRIMARY KEY (id);


--
-- Name: f_7eb4617384134a47982d7eee19769d f_7eb4617384134a47982d7eee19769d_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_7eb4617384134a47982d7eee19769d
    ADD CONSTRAINT f_7eb4617384134a47982d7eee19769d_pkey PRIMARY KEY (id);


--
-- Name: f_842802b2046c420b87a3d131633526 f_842802b2046c420b87a3d131633526_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_842802b2046c420b87a3d131633526
    ADD CONSTRAINT f_842802b2046c420b87a3d131633526_pkey PRIMARY KEY (id);


--
-- Name: f_88a663104e2e4eb58f77c06d2c2480 f_88a663104e2e4eb58f77c06d2c2480_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_88a663104e2e4eb58f77c06d2c2480
    ADD CONSTRAINT f_88a663104e2e4eb58f77c06d2c2480_pkey PRIMARY KEY (id);


--
-- Name: f_8e49e2ec060746578e1fec042d6565 f_8e49e2ec060746578e1fec042d6565_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_8e49e2ec060746578e1fec042d6565
    ADD CONSTRAINT f_8e49e2ec060746578e1fec042d6565_pkey PRIMARY KEY (id);


--
-- Name: f_9081d33eaa78434bbee51ed915a94c f_9081d33eaa78434bbee51ed915a94c_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_9081d33eaa78434bbee51ed915a94c
    ADD CONSTRAINT f_9081d33eaa78434bbee51ed915a94c_pkey PRIMARY KEY (id);


--
-- Name: f_91ecb37cc23e4c8a86fa08c9902d80 f_91ecb37cc23e4c8a86fa08c9902d80_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80
    ADD CONSTRAINT f_91ecb37cc23e4c8a86fa08c9902d80_pkey PRIMARY KEY (id);


--
-- Name: f_99e1849671054f0eb34effaefe2064 f_99e1849671054f0eb34effaefe2064_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_99e1849671054f0eb34effaefe2064
    ADD CONSTRAINT f_99e1849671054f0eb34effaefe2064_pkey PRIMARY KEY (id);


--
-- Name: f_a24524f50a444a4689645403db2ef8 f_a24524f50a444a4689645403db2ef8_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a24524f50a444a4689645403db2ef8
    ADD CONSTRAINT f_a24524f50a444a4689645403db2ef8_pkey PRIMARY KEY (id);


--
-- Name: f_a3da4861d02a4adba10e55b9ab9e6e f_a3da4861d02a4adba10e55b9ab9e6e_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e
    ADD CONSTRAINT f_a3da4861d02a4adba10e55b9ab9e6e_pkey PRIMARY KEY (id);


--
-- Name: f_a5e612cd6d394f28802adebcac250a f_a5e612cd6d394f28802adebcac250a_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a5e612cd6d394f28802adebcac250a
    ADD CONSTRAINT f_a5e612cd6d394f28802adebcac250a_pkey PRIMARY KEY (id);


--
-- Name: f_a7d1c02731ec419082a7277fe13cc0 f_a7d1c02731ec419082a7277fe13cc0_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_a7d1c02731ec419082a7277fe13cc0
    ADD CONSTRAINT f_a7d1c02731ec419082a7277fe13cc0_pkey PRIMARY KEY (id);


--
-- Name: f_aef913e8ecd147299348eb5e9a629f f_aef913e8ecd147299348eb5e9a629f_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_aef913e8ecd147299348eb5e9a629f
    ADD CONSTRAINT f_aef913e8ecd147299348eb5e9a629f_pkey PRIMARY KEY (id);


--
-- Name: f_b1d980c8b69441dc9ff24b14237f11 f_b1d980c8b69441dc9ff24b14237f11_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b1d980c8b69441dc9ff24b14237f11
    ADD CONSTRAINT f_b1d980c8b69441dc9ff24b14237f11_pkey PRIMARY KEY (id);


--
-- Name: f_b25a913184af4c39bd069076ccee9c f_b25a913184af4c39bd069076ccee9c_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b25a913184af4c39bd069076ccee9c
    ADD CONSTRAINT f_b25a913184af4c39bd069076ccee9c_pkey PRIMARY KEY (id);


--
-- Name: f_b3491f0025d34f0194be96e4547e36 f_b3491f0025d34f0194be96e4547e36_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b3491f0025d34f0194be96e4547e36
    ADD CONSTRAINT f_b3491f0025d34f0194be96e4547e36_pkey PRIMARY KEY (id);


--
-- Name: f_b3d1f9032ad243d18e768a8d7e3f76 f_b3d1f9032ad243d18e768a8d7e3f76_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76
    ADD CONSTRAINT f_b3d1f9032ad243d18e768a8d7e3f76_pkey PRIMARY KEY (id);


--
-- Name: f_b4d2b4619455408e9ba0f02386e539 f_b4d2b4619455408e9ba0f02386e539_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b4d2b4619455408e9ba0f02386e539
    ADD CONSTRAINT f_b4d2b4619455408e9ba0f02386e539_pkey PRIMARY KEY (id);


--
-- Name: f_b4e74e66d4bf4a428824a10431e0d7 f_b4e74e66d4bf4a428824a10431e0d7_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b4e74e66d4bf4a428824a10431e0d7
    ADD CONSTRAINT f_b4e74e66d4bf4a428824a10431e0d7_pkey PRIMARY KEY (id);


--
-- Name: f_b7052d76f72148b9aecbf08f7d300f f_b7052d76f72148b9aecbf08f7d300f_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b7052d76f72148b9aecbf08f7d300f
    ADD CONSTRAINT f_b7052d76f72148b9aecbf08f7d300f_pkey PRIMARY KEY (id);


--
-- Name: f_b7dc2be4209a496aa66e85f457a443 f_b7dc2be4209a496aa66e85f457a443_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b7dc2be4209a496aa66e85f457a443
    ADD CONSTRAINT f_b7dc2be4209a496aa66e85f457a443_pkey PRIMARY KEY (id);


--
-- Name: f_b9d3d3026af049c2a22e3bb79a3869 f_b9d3d3026af049c2a22e3bb79a3869_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b9d3d3026af049c2a22e3bb79a3869
    ADD CONSTRAINT f_b9d3d3026af049c2a22e3bb79a3869_pkey PRIMARY KEY (id);


--
-- Name: f_b9f108f222074be5b2d7bfac0705b3 f_b9f108f222074be5b2d7bfac0705b3_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_b9f108f222074be5b2d7bfac0705b3
    ADD CONSTRAINT f_b9f108f222074be5b2d7bfac0705b3_pkey PRIMARY KEY (id);


--
-- Name: f_bd44b7714c7f4dc2be4ab9c23e44c3 f_bd44b7714c7f4dc2be4ab9c23e44c3_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3
    ADD CONSTRAINT f_bd44b7714c7f4dc2be4ab9c23e44c3_pkey PRIMARY KEY (id);


--
-- Name: f_bf7c637287f149348dbe268887459a f_bf7c637287f149348dbe268887459a_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_bf7c637287f149348dbe268887459a
    ADD CONSTRAINT f_bf7c637287f149348dbe268887459a_pkey PRIMARY KEY (id);


--
-- Name: f_c00802c241d6413b8b1bc15677a816 f_c00802c241d6413b8b1bc15677a816_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_c00802c241d6413b8b1bc15677a816
    ADD CONSTRAINT f_c00802c241d6413b8b1bc15677a816_pkey PRIMARY KEY (id);


--
-- Name: f_c275409be23c4ffcae31cf9346077e f_c275409be23c4ffcae31cf9346077e_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_c275409be23c4ffcae31cf9346077e
    ADD CONSTRAINT f_c275409be23c4ffcae31cf9346077e_pkey PRIMARY KEY (id);


--
-- Name: f_c6c00aba44cd4932a704eb64605d30 f_c6c00aba44cd4932a704eb64605d30_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_c6c00aba44cd4932a704eb64605d30
    ADD CONSTRAINT f_c6c00aba44cd4932a704eb64605d30_pkey PRIMARY KEY (id);


--
-- Name: f_cb5c84470af44206942d56e73f2537 f_cb5c84470af44206942d56e73f2537_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cb5c84470af44206942d56e73f2537
    ADD CONSTRAINT f_cb5c84470af44206942d56e73f2537_pkey PRIMARY KEY (id);


--
-- Name: f_cb7eed03815440278883a83e21d29a f_cb7eed03815440278883a83e21d29a_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cb7eed03815440278883a83e21d29a
    ADD CONSTRAINT f_cb7eed03815440278883a83e21d29a_pkey PRIMARY KEY (id);


--
-- Name: f_cd23e938324b47699975c875199bc9 f_cd23e938324b47699975c875199bc9_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cd23e938324b47699975c875199bc9
    ADD CONSTRAINT f_cd23e938324b47699975c875199bc9_pkey PRIMARY KEY (id);


--
-- Name: f_cef1ce78dbf649c2a5b936227217db f_cef1ce78dbf649c2a5b936227217db_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_cef1ce78dbf649c2a5b936227217db
    ADD CONSTRAINT f_cef1ce78dbf649c2a5b936227217db_pkey PRIMARY KEY (id);


--
-- Name: f_dd21a32dab4d4971851c4ed1f7aae9 f_dd21a32dab4d4971851c4ed1f7aae9_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9
    ADD CONSTRAINT f_dd21a32dab4d4971851c4ed1f7aae9_pkey PRIMARY KEY (id);


--
-- Name: f_ddc3ac5aa4c54101bf2ac019495a37 f_ddc3ac5aa4c54101bf2ac019495a37_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37
    ADD CONSTRAINT f_ddc3ac5aa4c54101bf2ac019495a37_pkey PRIMARY KEY (id);


--
-- Name: f_e65e35e598e243419bcb96e759b2eb f_e65e35e598e243419bcb96e759b2eb_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_e65e35e598e243419bcb96e759b2eb
    ADD CONSTRAINT f_e65e35e598e243419bcb96e759b2eb_pkey PRIMARY KEY (id);


--
-- Name: f_e93241238a0f4153a9c8d648e55662 f_e93241238a0f4153a9c8d648e55662_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_e93241238a0f4153a9c8d648e55662
    ADD CONSTRAINT f_e93241238a0f4153a9c8d648e55662_pkey PRIMARY KEY (id);


--
-- Name: f_ea9d7de99bb2447986b4b4849eeb81 f_ea9d7de99bb2447986b4b4849eeb81_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_ea9d7de99bb2447986b4b4849eeb81
    ADD CONSTRAINT f_ea9d7de99bb2447986b4b4849eeb81_pkey PRIMARY KEY (id);


--
-- Name: f_f22a1a11a1134a86ab6e32ad52ccf3 f_f22a1a11a1134a86ab6e32ad52ccf3_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3
    ADD CONSTRAINT f_f22a1a11a1134a86ab6e32ad52ccf3_pkey PRIMARY KEY (id);


--
-- Name: f_f2b98a9b43ac45778f17d27444ad6d f_f2b98a9b43ac45778f17d27444ad6d_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f2b98a9b43ac45778f17d27444ad6d
    ADD CONSTRAINT f_f2b98a9b43ac45778f17d27444ad6d_pkey PRIMARY KEY (id);


--
-- Name: f_f5431315d3d149d2a707d5c829b703 f_f5431315d3d149d2a707d5c829b703_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f5431315d3d149d2a707d5c829b703
    ADD CONSTRAINT f_f5431315d3d149d2a707d5c829b703_pkey PRIMARY KEY (id);


--
-- Name: f_f782eda898d94749947dd8ba1ced20 f_f782eda898d94749947dd8ba1ced20_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f782eda898d94749947dd8ba1ced20
    ADD CONSTRAINT f_f782eda898d94749947dd8ba1ced20_pkey PRIMARY KEY (id);


--
-- Name: f_f8dfa6923e2d46a5afb5cb3c778894 f_f8dfa6923e2d46a5afb5cb3c778894_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894
    ADD CONSTRAINT f_f8dfa6923e2d46a5afb5cb3c778894_pkey PRIMARY KEY (id);


--
-- Name: f_f9e0fe869ca14d4ebf8cf503df4ea5 f_f9e0fe869ca14d4ebf8cf503df4ea5_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5
    ADD CONSTRAINT f_f9e0fe869ca14d4ebf8cf503df4ea5_pkey PRIMARY KEY (id);


--
-- Name: f_fb1438f3390a438d96a21b870e3319 f_fb1438f3390a438d96a21b870e3319_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_fb1438f3390a438d96a21b870e3319
    ADD CONSTRAINT f_fb1438f3390a438d96a21b870e3319_pkey PRIMARY KEY (id);


--
-- Name: f_fba08cfc6e534e778af5888e6cabd6 f_fba08cfc6e534e778af5888e6cabd6_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_fba08cfc6e534e778af5888e6cabd6
    ADD CONSTRAINT f_fba08cfc6e534e778af5888e6cabd6_pkey PRIMARY KEY (id);


--
-- Name: f_fd18eeb663824a85aa6b05987c781c f_fd18eeb663824a85aa6b05987c781c_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.f_fd18eeb663824a85aa6b05987c781c
    ADD CONSTRAINT f_fd18eeb663824a85aa6b05987c781c_pkey PRIMARY KEY (id);


--
-- Name: feature_preprocessing feature_preprocessing_project_feature_unique; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.feature_preprocessing
    ADD CONSTRAINT feature_preprocessing_project_feature_unique UNIQUE (project_id, feature_unique_id);


--
-- Name: grid_boundary_edges grid_boundary_edges_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.grid_boundary_edges
    ADD CONSTRAINT grid_boundary_edges_pkey PRIMARY KEY (planning_unit_id, h3_a, h3_b);


--
-- Name: h3_cells h3_cells_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.h3_cells
    ADD CONSTRAINT h3_cells_pkey PRIMARY KEY (h3_index, project_area, resolution);


--
-- Name: ices_ecoregions ices_ecoregions_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.ices_ecoregions
    ADD CONSTRAINT ices_ecoregions_pkey PRIMARY KEY (ogc_fid);


--
-- Name: jncc_sensitivities jncc_sensitivities_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.jncc_sensitivities
    ADD CONSTRAINT jncc_sensitivities_pkey PRIMARY KEY (id);


--
-- Name: metadata_activities metadata_activities_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_activities
    ADD CONSTRAINT metadata_activities_pkey PRIMARY KEY (id);


--
-- Name: metadata_interest_features metadata_interest_features_alias_unique_key; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_interest_features
    ADD CONSTRAINT metadata_interest_features_alias_unique_key UNIQUE (alias);


--
-- Name: metadata_interest_features metadata_interest_features_fcn_unique_key; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_interest_features
    ADD CONSTRAINT metadata_interest_features_fcn_unique_key UNIQUE (feature_class_name);


--
-- Name: metadata_interest_features metadata_interest_features_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_interest_features
    ADD CONSTRAINT metadata_interest_features_pkey PRIMARY KEY (unique_id);


--
-- Name: metadata_planning_units metadata_planning_units_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_planning_units
    ADD CONSTRAINT metadata_planning_units_pkey PRIMARY KEY (unique_id);


--
-- Name: metadata_planning_units metadata_planning_units_unique_constraint_02; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_planning_units
    ADD CONSTRAINT metadata_planning_units_unique_constraint_02 UNIQUE (alias);


--
-- Name: metadata_planning_units mpu02; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_planning_units
    ADD CONSTRAINT mpu02 UNIQUE (feature_class_name);


--
-- Name: pressures pressures_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pressures
    ADD CONSTRAINT pressures_pkey PRIMARY KEY (id);


--
-- Name: prioritizr_run_logs prioritizr_run_logs_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_run_logs
    ADD CONSTRAINT prioritizr_run_logs_pkey PRIMARY KEY (id);


--
-- Name: prioritizr_run_results prioritizr_run_results_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_run_results
    ADD CONSTRAINT prioritizr_run_results_pkey PRIMARY KEY (run_id, h3_index);


--
-- Name: prioritizr_runs prioritizr_runs_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_runs
    ADD CONSTRAINT prioritizr_runs_pkey PRIMARY KEY (id);


--
-- Name: project_features project_feature_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_features
    ADD CONSTRAINT project_feature_pkey PRIMARY KEY (project_id, feature_unique_id);


--
-- Name: project_files project_files_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_files
    ADD CONSTRAINT project_files_pkey PRIMARY KEY (id);


--
-- Name: project_metadata project_metadata_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_metadata
    ADD CONSTRAINT project_metadata_pkey PRIMARY KEY (id);


--
-- Name: project_pus project_pus_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_pus
    ADD CONSTRAINT project_pus_pkey PRIMARY KEY (id);


--
-- Name: project_pus project_pus_project_id_h3_index_key; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_pus
    ADD CONSTRAINT project_pus_project_id_h3_index_key UNIQUE (project_id, h3_index);


--
-- Name: project_renderer project_renderer_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_renderer
    ADD CONSTRAINT project_renderer_pkey PRIMARY KEY (id);


--
-- Name: project_run_parameters project_run_parameters_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_run_parameters
    ADD CONSTRAINT project_run_parameters_pkey PRIMARY KEY (id);


--
-- Name: projects projects_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.projects
    ADD CONSTRAINT projects_pkey PRIMARY KEY (id);


--
-- Name: pu_feature_amounts pu_feature_amounts_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pu_feature_amounts
    ADD CONSTRAINT pu_feature_amounts_pkey PRIMARY KEY (project_id, feature_unique_id, h3_index);


--
-- Name: pu_h3 pu_h3_pk; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pu_h3
    ADD CONSTRAINT pu_h3_pk PRIMARY KEY (pu_id, h3_index, resolution);


--
-- Name: res9_costs_staging res9_costs_staging_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.res9_costs_staging
    ADD CONSTRAINT res9_costs_staging_pkey PRIMARY KEY (h3_index);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (version);


--
-- Name: sensitivity_matrix sensitivity_matrix_eunis_code_pressure_key; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.sensitivity_matrix
    ADD CONSTRAINT sensitivity_matrix_eunis_code_pressure_key UNIQUE (eunis_code, pressure);


--
-- Name: sensitivity_matrix sensitivity_matrix_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.sensitivity_matrix
    ADD CONSTRAINT sensitivity_matrix_pkey PRIMARY KEY (id);


--
-- Name: species_data species_data_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.species_data
    ADD CONSTRAINT species_data_pkey PRIMARY KEY (id);


--
-- Name: user_projects user_projects_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.user_projects
    ADD CONSTRAINT user_projects_pkey PRIMARY KEY (user_id, project_id);


--
-- Name: users users_email_key; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.users
    ADD CONSTRAINT users_email_key UNIQUE (email);


--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);


--
-- Name: users users_username_key; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.users
    ADD CONSTRAINT users_username_key UNIQUE (username);


--
-- Name: activity_0011a2c9745b489cb90400b_geometry_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX activity_0011a2c9745b489cb90400b_geometry_geom_idx ON bioprotect.activity_0011a2c9745b489cb90400b USING gist (geometry);


--
-- Name: activity_2712fe1f88d942ac9cd34a5_geometry_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX activity_2712fe1f88d942ac9cd34a5_geometry_geom_idx ON bioprotect.activity_2712fe1f88d942ac9cd34a5 USING gist (geometry);


--
-- Name: activity_37910b3f692d41959faa618_geometry_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX activity_37910b3f692d41959faa618_geometry_geom_idx ON bioprotect.activity_37910b3f692d41959faa618 USING gist (geometry);


--
-- Name: activity_d45578c468bd4f12a91bd67_geometry_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX activity_d45578c468bd4f12a91bd67_geometry_geom_idx ON bioprotect.activity_d45578c468bd4f12a91bd67 USING gist (geometry);


--
-- Name: activity_f939f8d4bee04500af1e172_geometry_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX activity_f939f8d4bee04500af1e172_geometry_geom_idx ON bioprotect.activity_f939f8d4bee04500af1e172 USING gist (geometry);


--
-- Name: cost_profile_values_profile_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX cost_profile_values_profile_idx ON bioprotect.cost_profile_values USING btree (cost_profile_id);


--
-- Name: cost_profile_values_unique; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE UNIQUE INDEX cost_profile_values_unique ON bioprotect.cost_profile_values USING btree (cost_profile_id, project_pu_id);


--
-- Name: cost_profiles_project_name_uniq; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE UNIQUE INDEX cost_profiles_project_name_uniq ON bioprotect.cost_profiles USING btree (project_id, lower(name));


--
-- Name: h3_cells_geom_gix; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX h3_cells_geom_gix ON bioprotect.h3_cells USING gist (geom);


--
-- Name: h3_cells_h3_index_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX h3_cells_h3_index_idx ON bioprotect.h3_cells USING btree (h3_index);


--
-- Name: h3_cells_project_area_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX h3_cells_project_area_idx ON bioprotect.h3_cells USING btree (project_area);


--
-- Name: h3_cells_resolution_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX h3_cells_resolution_idx ON bioprotect.h3_cells USING btree (resolution);


--
-- Name: h3_cells_scale_level_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX h3_cells_scale_level_idx ON bioprotect.h3_cells USING btree (scale_level);


--
-- Name: ices_ecoregions_geometry_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX ices_ecoregions_geometry_geom_idx ON bioprotect.ices_ecoregions USING gist (geometry);


--
-- Name: idx_071e8b7b93c142fea772a689ea60c21c; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_071e8b7b93c142fea772a689ea60c21c ON bioprotect.f_cb7eed03815440278883a83e21d29a USING gist (geometry);


--
-- Name: idx_07aabe1c4a444c848558a85786bec12b; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_07aabe1c4a444c848558a85786bec12b ON bioprotect.f_148a329e7dba44e8aea3c1151422ce USING gist (geometry);


--
-- Name: idx_0a41b33c7c294cf3babbda0a7a5cfc47; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_0a41b33c7c294cf3babbda0a7a5cfc47 ON bioprotect.f_459758257ec542739ee8c64554920e USING gist (geometry);


--
-- Name: idx_0af5edb60ae3416c89b5a77dbd5d5f66; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_0af5edb60ae3416c89b5a77dbd5d5f66 ON bioprotect.f_2f5e67d4c1274b1eaf9a5a626a4282 USING gist (geometry);


--
-- Name: idx_0b34fa9c60fa45688cd3be1cb6e1f0fa; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_0b34fa9c60fa45688cd3be1cb6e1f0fa ON bioprotect.f_ea9d7de99bb2447986b4b4849eeb81 USING gist (geometry);


--
-- Name: idx_0dcb220632654e088786fcbc8017970d; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_0dcb220632654e088786fcbc8017970d ON bioprotect.activity_d45578c468bd4f12a91bd67 USING gist (geometry);


--
-- Name: idx_0eb34ddc1eb1483a9b6b2625db8b4049; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_0eb34ddc1eb1483a9b6b2625db8b4049 ON bioprotect.f_b9f108f222074be5b2d7bfac0705b3 USING gist (geometry);


--
-- Name: idx_1166a5d5c8e24fbc9fa6ecc34722aa73; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_1166a5d5c8e24fbc9fa6ecc34722aa73 ON bioprotect.f_88a663104e2e4eb58f77c06d2c2480 USING gist (geometry);


--
-- Name: idx_13c698da1ec54440849e2f54dc88b2e6; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_13c698da1ec54440849e2f54dc88b2e6 ON bioprotect.f_bf7c637287f149348dbe268887459a USING gist (geometry);


--
-- Name: idx_1461cc8dcc134ae69edd4f9d7e484431; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_1461cc8dcc134ae69edd4f9d7e484431 ON bioprotect.f_cb5c84470af44206942d56e73f2537 USING gist (geometry);


--
-- Name: idx_17caa98a0635432db2bacc00d515ea25; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_17caa98a0635432db2bacc00d515ea25 ON bioprotect.f_9081d33eaa78434bbee51ed915a94c USING gist (geometry);


--
-- Name: idx_182b1fa4a6d84744b775de68709561fb; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_182b1fa4a6d84744b775de68709561fb ON bioprotect.f_52d03f922eb14788ae1bef30f1429e USING gist (geometry);


--
-- Name: idx_1b65fc1597c64db9ae1f8eb5cbad18ce; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_1b65fc1597c64db9ae1f8eb5cbad18ce ON bioprotect.f_e65e35e598e243419bcb96e759b2eb USING gist (geometry);


--
-- Name: idx_1e0b56e06a2340b3b3068f242884cb3a; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_1e0b56e06a2340b3b3068f242884cb3a ON bioprotect.f_fba08cfc6e534e778af5888e6cabd6 USING gist (geometry);


--
-- Name: idx_1ef34c02b28447309fb66e86e6e8a044; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_1ef34c02b28447309fb66e86e6e8a044 ON bioprotect.f_1ca7a0ceec044faa9e153e72fce960 USING gist (geometry);


--
-- Name: idx_20287e1612274500a0f8e3b5e63f6eb9; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_20287e1612274500a0f8e3b5e63f6eb9 ON bioprotect.f_fb1438f3390a438d96a21b870e3319 USING gist (geometry);


--
-- Name: idx_2345a22c4e1b4d578895c6d6d050badf; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_2345a22c4e1b4d578895c6d6d050badf ON bioprotect.activity_37910b3f692d41959faa618 USING gist (geometry);


--
-- Name: idx_2990531a4488480f8e9e671f5b979e30; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_2990531a4488480f8e9e671f5b979e30 ON bioprotect.f_7b6705b767494f6cb8c937e4929fd4 USING gist (geometry);


--
-- Name: idx_320f19bc9cf8458d9058f1ef953f3acb; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_320f19bc9cf8458d9058f1ef953f3acb ON bioprotect.f_f2b98a9b43ac45778f17d27444ad6d USING gist (geometry);


--
-- Name: idx_36bd544a826f4662bddff1bc9833c981; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_36bd544a826f4662bddff1bc9833c981 ON bioprotect.f_01889e7f71624c0d9f2ad2c3241708 USING gist (geometry);


--
-- Name: idx_36c9ca4210e44c95bfa72c2f879d5730; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_36c9ca4210e44c95bfa72c2f879d5730 ON bioprotect.f_b7dc2be4209a496aa66e85f457a443 USING gist (geometry);


--
-- Name: idx_3a6b916817794916ae655c5ec0cfca16; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_3a6b916817794916ae655c5ec0cfca16 ON bioprotect.f_717bb7ba09ca4e38a18bdd04a88cc2 USING gist (geometry);


--
-- Name: idx_3ec198b1418d4c908593e1c3e5e494a5; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_3ec198b1418d4c908593e1c3e5e494a5 ON bioprotect.f_b1d980c8b69441dc9ff24b14237f11 USING gist (geometry);


--
-- Name: idx_4212327e63b242ee8cdd6a0d51f30927; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_4212327e63b242ee8cdd6a0d51f30927 ON bioprotect.f_b4d2b4619455408e9ba0f02386e539 USING gist (geometry);


--
-- Name: idx_46b0e987933a40d5891ae5b9baef71bc; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_46b0e987933a40d5891ae5b9baef71bc ON bioprotect.f_aef913e8ecd147299348eb5e9a629f USING gist (geometry);


--
-- Name: idx_4789f2ddc69b4d60883508ba362bed91; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_4789f2ddc69b4d60883508ba362bed91 ON bioprotect.f_842802b2046c420b87a3d131633526 USING gist (geometry);


--
-- Name: idx_48ae67fd5bfa4a72bb2561a70735063a; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_48ae67fd5bfa4a72bb2561a70735063a ON bioprotect.f_b4e74e66d4bf4a428824a10431e0d7 USING gist (geometry);


--
-- Name: idx_498c81ca725141178be7898cfdb67691; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_498c81ca725141178be7898cfdb67691 ON bioprotect.f_f5431315d3d149d2a707d5c829b703 USING gist (geometry);


--
-- Name: idx_4dd6f92689164d3eb8fc9e9c053c1daa; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_4dd6f92689164d3eb8fc9e9c053c1daa ON bioprotect.f_7c1b92efed5443b78dd4d0d09121c9 USING gist (geometry);


--
-- Name: idx_4e455db4102a4121aa12d5e86d29b7d7; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_4e455db4102a4121aa12d5e86d29b7d7 ON bioprotect.f_99e1849671054f0eb34effaefe2064 USING gist (geometry);


--
-- Name: idx_4ee9e12868004a5ba3ea647e2e039cfc; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_4ee9e12868004a5ba3ea647e2e039cfc ON bioprotect.f_0b85bb11b41a4e269c0aebfbe0b544 USING gist (geometry);


--
-- Name: idx_4f8833f1fc0f4b12b09df6f42a70514b; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_4f8833f1fc0f4b12b09df6f42a70514b ON bioprotect.f_1f528ee9c1ee4a37a2e1ebbb1af2a9 USING gist (geometry);


--
-- Name: idx_503e66879c9f4e5a9f188f87737926d2; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_503e66879c9f4e5a9f188f87737926d2 ON bioprotect.f_245a2235f8d642269a1aaa82bedcb5 USING gist (geometry);


--
-- Name: idx_5055befbdb174027b679b2a8b42ea63a; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_5055befbdb174027b679b2a8b42ea63a ON bioprotect.f_b7052d76f72148b9aecbf08f7d300f USING gist (geometry);


--
-- Name: idx_585ed63306cd4e3dbd85810eefb5e0b4; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_585ed63306cd4e3dbd85810eefb5e0b4 ON bioprotect.f_187731fcf47b4d2e982a38b5532a3a USING gist (geometry);


--
-- Name: idx_609b30ad06b6423d89f6a7b0aa6ee697; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_609b30ad06b6423d89f6a7b0aa6ee697 ON bioprotect.f_2ec291545abc47aea06b69f21e192c USING gist (geometry);


--
-- Name: idx_6699e772bf1649e0b9c3ee0f9dc6b8c3; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_6699e772bf1649e0b9c3ee0f9dc6b8c3 ON bioprotect.f_b3491f0025d34f0194be96e4547e36 USING gist (geometry);


--
-- Name: idx_69b83d79f1dd49729d73c541585e4486; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_69b83d79f1dd49729d73c541585e4486 ON bioprotect.f_0c897459e69d4c22a1c00299aa5547 USING gist (geometry);


--
-- Name: idx_6aef7453c184402aae7d2aa79e88e0db; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_6aef7453c184402aae7d2aa79e88e0db ON bioprotect.activity_0011a2c9745b489cb90400b USING gist (geometry);


--
-- Name: idx_6af2d750cb144c85a826c4139faa22dd; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_6af2d750cb144c85a826c4139faa22dd ON bioprotect.f_91ecb37cc23e4c8a86fa08c9902d80 USING gist (geometry);


--
-- Name: idx_6b392b4bfddc49838573bb3f9cad40d4; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_6b392b4bfddc49838573bb3f9cad40d4 ON bioprotect.f_f782eda898d94749947dd8ba1ced20 USING gist (geometry);


--
-- Name: idx_6e64ba08b0b0443981c1ab61f1f5dbc2; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_6e64ba08b0b0443981c1ab61f1f5dbc2 ON bioprotect.f_a3da4861d02a4adba10e55b9ab9e6e USING gist (geometry);


--
-- Name: idx_6ef91afdc4db45199c9c6f566046c274; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_6ef91afdc4db45199c9c6f566046c274 ON bioprotect.f_26a73e4c6eb0425e8c998ce0cb84b4 USING gist (geometry);


--
-- Name: idx_741823bc3e6e49f09b137a2a40af16a8; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_741823bc3e6e49f09b137a2a40af16a8 ON bioprotect.f_f22a1a11a1134a86ab6e32ad52ccf3 USING gist (geometry);


--
-- Name: idx_78b50eb18c9c4ffd8be1805c335330a5; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_78b50eb18c9c4ffd8be1805c335330a5 ON bioprotect.f_7eb4617384134a47982d7eee19769d USING gist (geometry);


--
-- Name: idx_7be36b7040fb408bbc2f5290a749fd4b; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_7be36b7040fb408bbc2f5290a749fd4b ON bioprotect.f_c275409be23c4ffcae31cf9346077e USING gist (geometry);


--
-- Name: idx_7c77d0156e3043a0bbd295dcc262a4e1; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_7c77d0156e3043a0bbd295dcc262a4e1 ON bioprotect.f_bd44b7714c7f4dc2be4ab9c23e44c3 USING gist (geometry);


--
-- Name: idx_7d5e61d69ac44711906991a6f461ef9f; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_7d5e61d69ac44711906991a6f461ef9f ON bioprotect.f_a5e612cd6d394f28802adebcac250a USING gist (geometry);


--
-- Name: idx_7f2cb65cad854587a5a155bcf2a94367; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_7f2cb65cad854587a5a155bcf2a94367 ON bioprotect.f_2ccd7d878404486c8b8b9d5c90e9fc USING gist (geometry);


--
-- Name: idx_7ff7f0c5af1948c7a8eb15d972f69a8a; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_7ff7f0c5af1948c7a8eb15d972f69a8a ON bioprotect.f_234e530d24fc4252bed2b3e84ee1ba USING gist (geometry);


--
-- Name: idx_83e72fea229345cf9d62a4c178864a4b; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_83e72fea229345cf9d62a4c178864a4b ON bioprotect.f_f9e0fe869ca14d4ebf8cf503df4ea5 USING gist (geometry);


--
-- Name: idx_85240495076d4406966dc41fb4ccf675; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_85240495076d4406966dc41fb4ccf675 ON bioprotect.f_cef1ce78dbf649c2a5b936227217db USING gist (geometry);


--
-- Name: idx_8a09c24500ab48ea9ef6a56458740799; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_8a09c24500ab48ea9ef6a56458740799 ON bioprotect.f_3061a71a8f0244848fbbff758c198e USING gist (geometry);


--
-- Name: idx_8f4e170ba26b4daf8b31f78bab140563; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_8f4e170ba26b4daf8b31f78bab140563 ON bioprotect.f_c00802c241d6413b8b1bc15677a816 USING gist (geometry);


--
-- Name: idx_9845151bdfa94e3bb9cb4532f930b733; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_9845151bdfa94e3bb9cb4532f930b733 ON bioprotect.activity_2712fe1f88d942ac9cd34a5 USING gist (geometry);


--
-- Name: idx_994967caf09840f88ad82d0ddf5bb894; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_994967caf09840f88ad82d0ddf5bb894 ON bioprotect.f_352d7d971b674f06bcc2eb894c8685 USING gist (geometry);


--
-- Name: idx_9b22d769243a415780e3340502d49e16; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_9b22d769243a415780e3340502d49e16 ON bioprotect.f_3bcc07d1d7e142ddbe995cee1c2060 USING gist (geometry);


--
-- Name: idx_ab2b70888b8f47bfb23c3e6793704d68; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_ab2b70888b8f47bfb23c3e6793704d68 ON bioprotect.f_fd18eeb663824a85aa6b05987c781c USING gist (geometry);


--
-- Name: idx_ac4a0117925c4321acc7f57697b1b025; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_ac4a0117925c4321acc7f57697b1b025 ON bioprotect.f_4dcb01bda5214973ae6d3d2f02982f USING gist (geometry);


--
-- Name: idx_ae18723292684a389a76f4641696ab14; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_ae18723292684a389a76f4641696ab14 ON bioprotect.f_b25a913184af4c39bd069076ccee9c USING gist (geometry);


--
-- Name: idx_af987b267726412ca059c41eb907dab7; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_af987b267726412ca059c41eb907dab7 ON bioprotect.f_21debeb092f844f2b64b001bd64c29 USING gist (geometry);


--
-- Name: idx_baac846d0481460d905e04b5f72ba029; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_baac846d0481460d905e04b5f72ba029 ON bioprotect.f_37a68a8282d3495daee95f63fb8f6f USING gist (geometry);


--
-- Name: idx_be722b87211e436d881348ba3fca38a7; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_be722b87211e436d881348ba3fca38a7 ON bioprotect.f_7a5e4445f2a248d798a1e1a2b3d8c1 USING gist (geometry);


--
-- Name: idx_bioprotect_prioritizr_input_run_10_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_10_pu ON bioprotect.prioritizr_input_run_10 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_11_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_11_pu ON bioprotect.prioritizr_input_run_11 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_12_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_12_pu ON bioprotect.prioritizr_input_run_12 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_13_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_13_pu ON bioprotect.prioritizr_input_run_13 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_14_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_14_pu ON bioprotect.prioritizr_input_run_14 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_16_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_16_pu ON bioprotect.prioritizr_input_run_16 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_17_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_17_pu ON bioprotect.prioritizr_input_run_17 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_18_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_18_pu ON bioprotect.prioritizr_input_run_18 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_19_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_19_pu ON bioprotect.prioritizr_input_run_19 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_1_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_1_pu ON bioprotect.prioritizr_input_run_1 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_20_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_20_pu ON bioprotect.prioritizr_input_run_20 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_21_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_21_pu ON bioprotect.prioritizr_input_run_21 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_22_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_22_pu ON bioprotect.prioritizr_input_run_22 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_23_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_23_pu ON bioprotect.prioritizr_input_run_23 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_24_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_24_pu ON bioprotect.prioritizr_input_run_24 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_25_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_25_pu ON bioprotect.prioritizr_input_run_25 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_2_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_2_pu ON bioprotect.prioritizr_input_run_2 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_31_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_31_pu ON bioprotect.prioritizr_input_run_31 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_32_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_32_pu ON bioprotect.prioritizr_input_run_32 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_33_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_33_pu ON bioprotect.prioritizr_input_run_33 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_34_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_34_pu ON bioprotect.prioritizr_input_run_34 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_35_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_35_pu ON bioprotect.prioritizr_input_run_35 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_36_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_36_pu ON bioprotect.prioritizr_input_run_36 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_37_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_37_pu ON bioprotect.prioritizr_input_run_37 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_38_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_38_pu ON bioprotect.prioritizr_input_run_38 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_39_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_39_pu ON bioprotect.prioritizr_input_run_39 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_3_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_3_pu ON bioprotect.prioritizr_input_run_3 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_40_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_40_pu ON bioprotect.prioritizr_input_run_40 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_41_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_41_pu ON bioprotect.prioritizr_input_run_41 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_42_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_42_pu ON bioprotect.prioritizr_input_run_42 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_43_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_43_pu ON bioprotect.prioritizr_input_run_43 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_44_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_44_pu ON bioprotect.prioritizr_input_run_44 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_45_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_45_pu ON bioprotect.prioritizr_input_run_45 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_46_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_46_pu ON bioprotect.prioritizr_input_run_46 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_47_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_47_pu ON bioprotect.prioritizr_input_run_47 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_48_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_48_pu ON bioprotect.prioritizr_input_run_48 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_49_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_49_pu ON bioprotect.prioritizr_input_run_49 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_4_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_4_pu ON bioprotect.prioritizr_input_run_4 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_50_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_50_pu ON bioprotect.prioritizr_input_run_50 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_51_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_51_pu ON bioprotect.prioritizr_input_run_51 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_52_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_52_pu ON bioprotect.prioritizr_input_run_52 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_53_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_53_pu ON bioprotect.prioritizr_input_run_53 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_54_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_54_pu ON bioprotect.prioritizr_input_run_54 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_55_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_55_pu ON bioprotect.prioritizr_input_run_55 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_56_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_56_pu ON bioprotect.prioritizr_input_run_56 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_57_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_57_pu ON bioprotect.prioritizr_input_run_57 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_58_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_58_pu ON bioprotect.prioritizr_input_run_58 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_59_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_59_pu ON bioprotect.prioritizr_input_run_59 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_5_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_5_pu ON bioprotect.prioritizr_input_run_5 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_60_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_60_pu ON bioprotect.prioritizr_input_run_60 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_61_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_61_pu ON bioprotect.prioritizr_input_run_61 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_62_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_62_pu ON bioprotect.prioritizr_input_run_62 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_63_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_63_pu ON bioprotect.prioritizr_input_run_63 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_64_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_64_pu ON bioprotect.prioritizr_input_run_64 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_65_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_65_pu ON bioprotect.prioritizr_input_run_65 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_66_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_66_pu ON bioprotect.prioritizr_input_run_66 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_67_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_67_pu ON bioprotect.prioritizr_input_run_67 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_68_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_68_pu ON bioprotect.prioritizr_input_run_68 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_69_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_69_pu ON bioprotect.prioritizr_input_run_69 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_6_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_6_pu ON bioprotect.prioritizr_input_run_6 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_70_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_70_pu ON bioprotect.prioritizr_input_run_70 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_71_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_71_pu ON bioprotect.prioritizr_input_run_71 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_72_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_72_pu ON bioprotect.prioritizr_input_run_72 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_73_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_73_pu ON bioprotect.prioritizr_input_run_73 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_74_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_74_pu ON bioprotect.prioritizr_input_run_74 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_75_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_75_pu ON bioprotect.prioritizr_input_run_75 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_76_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_76_pu ON bioprotect.prioritizr_input_run_76 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_77_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_77_pu ON bioprotect.prioritizr_input_run_77 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_78_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_78_pu ON bioprotect.prioritizr_input_run_78 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_79_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_79_pu ON bioprotect.prioritizr_input_run_79 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_7_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_7_pu ON bioprotect.prioritizr_input_run_7 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_80_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_80_pu ON bioprotect.prioritizr_input_run_80 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_81_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_81_pu ON bioprotect.prioritizr_input_run_81 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_82_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_82_pu ON bioprotect.prioritizr_input_run_82 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_8_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_8_pu ON bioprotect.prioritizr_input_run_8 USING btree (pu_id);


--
-- Name: idx_bioprotect_prioritizr_input_run_9_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_bioprotect_prioritizr_input_run_9_pu ON bioprotect.prioritizr_input_run_9 USING btree (pu_id);


--
-- Name: idx_c2d2ae9525c04b94821f408583e377dc; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_c2d2ae9525c04b94821f408583e377dc ON bioprotect.f_2187b5a975fe402e8f9a17cfa55eac USING gist (geometry);


--
-- Name: idx_c62f42234cd6476d8f3fc058fb783ea0; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_c62f42234cd6476d8f3fc058fb783ea0 ON bioprotect.f_2a587cadbd434a72b87ff6a2c3cc77 USING gist (geometry);


--
-- Name: idx_ca75670e15894ed0919f248fe396eaa6; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_ca75670e15894ed0919f248fe396eaa6 ON bioprotect.f_e93241238a0f4153a9c8d648e55662 USING gist (geometry);


--
-- Name: idx_d20441228d794342bf9376b0db6b6e22; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_d20441228d794342bf9376b0db6b6e22 ON bioprotect.f_65f429eb64904e2a8c4d4f55a13cc9 USING gist (geometry);


--
-- Name: idx_d453becfd707420bb08c195ecb5a5be1; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_d453becfd707420bb08c195ecb5a5be1 ON bioprotect.f_ddc3ac5aa4c54101bf2ac019495a37 USING gist (geometry);


--
-- Name: idx_d67d477c963f46a589deec1c4933fe4b; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_d67d477c963f46a589deec1c4933fe4b ON bioprotect.f_cd23e938324b47699975c875199bc9 USING gist (geometry);


--
-- Name: idx_d8736c438cb043ef9b45d94e79a0ee8b; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_d8736c438cb043ef9b45d94e79a0ee8b ON bioprotect.f_a24524f50a444a4689645403db2ef8 USING gist (geometry);


--
-- Name: idx_dde4d2dd908b4326878119a17f4412ba; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_dde4d2dd908b4326878119a17f4412ba ON bioprotect.activity_f939f8d4bee04500af1e172 USING gist (geometry);


--
-- Name: idx_e7f947f3fffe4456b778fd949f805151; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_e7f947f3fffe4456b778fd949f805151 ON bioprotect.f_6deb40e7592e45f79fe1eb99a5f590 USING gist (geometry);


--
-- Name: idx_ea5d324d056049dbb1849fd5f50b3469; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_ea5d324d056049dbb1849fd5f50b3469 ON bioprotect.f_3923082fbb6e4b0bb22fbca0530c2f USING gist (geometry);


--
-- Name: idx_ee144b6627bd4e5fa9f143b9dfe45fae; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_ee144b6627bd4e5fa9f143b9dfe45fae ON bioprotect.f_c6c00aba44cd4932a704eb64605d30 USING gist (geometry);


--
-- Name: idx_f0f2de3c0b634ce3b3ff431ed5d7fb00; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_f0f2de3c0b634ce3b3ff431ed5d7fb00 ON bioprotect.f_8e49e2ec060746578e1fec042d6565 USING gist (geometry);


--
-- Name: idx_f6d2369525b847d18ff979eb952caff5; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_f6d2369525b847d18ff979eb952caff5 ON bioprotect.f_f8dfa6923e2d46a5afb5cb3c778894 USING gist (geometry);


--
-- Name: idx_f716b4baaad042a7b97171ce1b50cdda; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_f716b4baaad042a7b97171ce1b50cdda ON bioprotect.f_b3d1f9032ad243d18e768a8d7e3f76 USING gist (geometry);


--
-- Name: idx_f76083f042f847e8be73fca43b50eb77; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_f76083f042f847e8be73fca43b50eb77 ON bioprotect.f_dd21a32dab4d4971851c4ed1f7aae9 USING gist (geometry);


--
-- Name: idx_f91d93429762437a8069b9ab4020a141; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_f91d93429762437a8069b9ab4020a141 ON bioprotect.f_b9d3d3026af049c2a22e3bb79a3869 USING gist (geometry);


--
-- Name: idx_fd5c2776f36446e0bd8f7d4c602c05b9; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_fd5c2776f36446e0bd8f7d4c602c05b9 ON bioprotect.f_a7d1c02731ec419082a7277fe13cc0 USING gist (geometry);


--
-- Name: idx_grid_boundary_edges_grid; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_grid_boundary_edges_grid ON bioprotect.grid_boundary_edges USING btree (planning_unit_id);


--
-- Name: idx_h3_cells_h3; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_h3_cells_h3 ON bioprotect.h3_cells USING btree (h3_index);


--
-- Name: idx_pf_feature; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_pf_feature ON bioprotect.project_features USING btree (feature_unique_id);


--
-- Name: idx_pf_project; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_pf_project ON bioprotect.project_features USING btree (project_id);


--
-- Name: idx_planning_unit_id; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_planning_unit_id ON bioprotect.projects USING btree (planning_unit_id);


--
-- Name: idx_pressures_activity_id; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_pressures_activity_id ON bioprotect.pressures USING btree (activity_id);


--
-- Name: idx_pressures_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_pressures_geom ON bioprotect.pressures USING gist (geometry);


--
-- Name: idx_pressures_pressuretitle; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_pressures_pressuretitle ON bioprotect.pressures USING btree (pressuretitle);


--
-- Name: idx_project_id; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_project_id ON bioprotect.project_run_parameters USING btree (project_id);


--
-- Name: idx_project_pus_project_h3; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_project_pus_project_h3 ON bioprotect.project_pus USING btree (project_id, h3_index);


--
-- Name: idx_pu_feature_amounts_project_feature; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_pu_feature_amounts_project_feature ON bioprotect.pu_feature_amounts USING btree (project_id, feature_unique_id);


--
-- Name: idx_pu_feature_amounts_puid; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_pu_feature_amounts_puid ON bioprotect.pu_feature_amounts USING btree (h3_index);


--
-- Name: idx_sensitivity_eunis; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_sensitivity_eunis ON bioprotect.sensitivity_matrix USING btree (eunis_code);


--
-- Name: idx_sensitivity_pressure; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_sensitivity_pressure ON bioprotect.sensitivity_matrix USING btree (pressure);


--
-- Name: idx_user_projects_project; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_user_projects_project ON bioprotect.user_projects USING btree (project_id);


--
-- Name: idx_user_projects_user; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_user_projects_user ON bioprotect.user_projects USING btree (user_id);


--
-- Name: idx_users_username; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_users_username ON bioprotect.users USING btree (username);


--
-- Name: idx_v_h3_adriatic_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_adriatic_sea_res7_geom ON bioprotect.v_h3_adriatic_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_aegean_levantine_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_aegean_levantine_sea_res7_geom ON bioprotect.v_h3_aegean_levantine_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_arctic_ocean_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_arctic_ocean_res7_geom ON bioprotect.v_h3_arctic_ocean_res7 USING gist (geometry);


--
-- Name: idx_v_h3_azores_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_azores_res7_geom ON bioprotect.v_h3_azores_res7 USING gist (geometry);


--
-- Name: idx_v_h3_baltic_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_baltic_sea_res7_geom ON bioprotect.v_h3_baltic_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_barents_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_barents_sea_res7_geom ON bioprotect.v_h3_barents_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_bay_of_biscay_and_the_iberian_coast_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_bay_of_biscay_and_the_iberian_coast_res7_geom ON bioprotect.v_h3_bay_of_biscay_and_the_iberian_coast_res7 USING gist (geometry);


--
-- Name: idx_v_h3_black_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_black_sea_res7_geom ON bioprotect.v_h3_black_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_celtic_seas_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_celtic_seas_res7_geom ON bioprotect.v_h3_celtic_seas_res7 USING gist (geometry);


--
-- Name: idx_v_h3_faroes_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_faroes_res7_geom ON bioprotect.v_h3_faroes_res7 USING gist (geometry);


--
-- Name: idx_v_h3_greater_north_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_greater_north_sea_res7_geom ON bioprotect.v_h3_greater_north_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_greenland_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_greenland_sea_res7_geom ON bioprotect.v_h3_greenland_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_icelandic_waters_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_icelandic_waters_res7_geom ON bioprotect.v_h3_icelandic_waters_res7 USING gist (geometry);


--
-- Name: idx_v_h3_ionian_sea_and_the_central_mediterranean_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_ionian_sea_and_the_central_mediterranean_sea_res7_geom ON bioprotect.v_h3_ionian_sea_and_the_central_mediterranean_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_msp_assessment_area_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_msp_assessment_area_res7_geom ON bioprotect.v_h3_msp_assessment_area_res7 USING gist (geometry);


--
-- Name: idx_v_h3_norwegian_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_norwegian_sea_res7_geom ON bioprotect.v_h3_norwegian_sea_res7 USING gist (geometry);


--
-- Name: idx_v_h3_oceanic_northeast_atlantic_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_oceanic_northeast_atlantic_res7_geom ON bioprotect.v_h3_oceanic_northeast_atlantic_res7 USING gist (geometry);


--
-- Name: idx_v_h3_western_mediterranean_sea_res7_geom; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_v_h3_western_mediterranean_sea_res7_geom ON bioprotect.v_h3_western_mediterranean_sea_res7 USING gist (geometry);


--
-- Name: ix_bioprotect_pad_index; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX ix_bioprotect_pad_index ON bioprotect.pad USING btree (index);


--
-- Name: mif_01; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX mif_01 ON bioprotect.metadata_interest_features USING btree (feature_class_name);


--
-- Name: mpu_01; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX mpu_01 ON bioprotect.metadata_planning_units USING btree (feature_class_name);


--
-- Name: project_feature_feature_unique_id_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX project_feature_feature_unique_id_idx ON bioprotect.project_features USING btree (feature_unique_id);


--
-- Name: v_h3_case_study_extents_water_only_res7_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX v_h3_case_study_extents_water_only_res7_geom_idx ON bioprotect.v_h3_case_study_extents_water_only_res7 USING gist (geometry);


--
-- Name: v_h3_case_study_extents_water_only_res9_geom_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX v_h3_case_study_extents_water_only_res9_geom_idx ON bioprotect.v_h3_case_study_extents_water_only_res9 USING gist (geometry);


--
-- Name: project_features trg_pf_updated; Type: TRIGGER; Schema: bioprotect; Owner: -
--

CREATE TRIGGER trg_pf_updated BEFORE UPDATE ON bioprotect.project_features FOR EACH ROW EXECUTE FUNCTION bioprotect.touch_updated_at();


--
-- Name: cost_profile_values cost_profile_values_cost_profile_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_values
    ADD CONSTRAINT cost_profile_values_cost_profile_id_fkey FOREIGN KEY (cost_profile_id) REFERENCES bioprotect.cost_profiles(id) ON DELETE CASCADE;


--
-- Name: cost_profile_values cost_profile_values_project_pu_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_values
    ADD CONSTRAINT cost_profile_values_project_pu_id_fkey FOREIGN KEY (project_pu_id) REFERENCES bioprotect.project_pus(id) ON DELETE CASCADE;


--
-- Name: cost_profiles cost_profiles_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profiles
    ADD CONSTRAINT cost_profiles_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: projects fk_planning_unit; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.projects
    ADD CONSTRAINT fk_planning_unit FOREIGN KEY (planning_unit_id) REFERENCES bioprotect.metadata_planning_units(unique_id) ON DELETE SET NULL;


--
-- Name: pressures pressures_activity_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pressures
    ADD CONSTRAINT pressures_activity_id_fkey FOREIGN KEY (activity_id) REFERENCES bioprotect.metadata_activities(id);


--
-- Name: prioritizr_run_logs prioritizr_run_logs_run_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_run_logs
    ADD CONSTRAINT prioritizr_run_logs_run_id_fkey FOREIGN KEY (run_id) REFERENCES bioprotect.prioritizr_runs(id) ON DELETE CASCADE;


--
-- Name: prioritizr_run_results prioritizr_run_results_run_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_run_results
    ADD CONSTRAINT prioritizr_run_results_run_id_fkey FOREIGN KEY (run_id) REFERENCES bioprotect.prioritizr_runs(id) ON DELETE CASCADE;


--
-- Name: prioritizr_runs prioritizr_runs_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.prioritizr_runs
    ADD CONSTRAINT prioritizr_runs_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id);


--
-- Name: project_features project_feature_feature_unique_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_features
    ADD CONSTRAINT project_feature_feature_unique_id_fkey FOREIGN KEY (feature_unique_id) REFERENCES bioprotect.metadata_interest_features(unique_id) ON DELETE CASCADE;


--
-- Name: project_features project_feature_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_features
    ADD CONSTRAINT project_feature_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: project_files project_files_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_files
    ADD CONSTRAINT project_files_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: project_metadata project_metadata_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_metadata
    ADD CONSTRAINT project_metadata_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id);


--
-- Name: project_pus project_pus_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_pus
    ADD CONSTRAINT project_pus_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: project_renderer project_renderer_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_renderer
    ADD CONSTRAINT project_renderer_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: project_run_parameters project_run_parameters_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.project_run_parameters
    ADD CONSTRAINT project_run_parameters_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: projects projects_active_cost_profile_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.projects
    ADD CONSTRAINT projects_active_cost_profile_id_fkey FOREIGN KEY (active_cost_profile_id) REFERENCES bioprotect.cost_profiles(id);


--
-- Name: pu_feature_amounts pu_feature_amounts_feature_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pu_feature_amounts
    ADD CONSTRAINT pu_feature_amounts_feature_fkey FOREIGN KEY (feature_unique_id) REFERENCES bioprotect.metadata_interest_features(unique_id) ON DELETE CASCADE;


--
-- Name: pu_feature_amounts pu_feature_amounts_project_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pu_feature_amounts
    ADD CONSTRAINT pu_feature_amounts_project_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: pu_h3 pu_h3_pu_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.pu_h3
    ADD CONSTRAINT pu_h3_pu_id_fkey FOREIGN KEY (pu_id) REFERENCES bioprotect.metadata_planning_units(unique_id);


--
-- Name: species_data species_data_feature_unique_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.species_data
    ADD CONSTRAINT species_data_feature_unique_id_fkey FOREIGN KEY (feature_unique_id) REFERENCES bioprotect.metadata_interest_features(unique_id) ON DELETE CASCADE;


--
-- Name: species_data species_data_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.species_data
    ADD CONSTRAINT species_data_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: user_projects user_projects_project_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.user_projects
    ADD CONSTRAINT user_projects_project_id_fkey FOREIGN KEY (project_id) REFERENCES bioprotect.projects(id) ON DELETE CASCADE;


--
-- Name: user_projects user_projects_user_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.user_projects
    ADD CONSTRAINT user_projects_user_id_fkey FOREIGN KEY (user_id) REFERENCES bioprotect.users(id) ON DELETE CASCADE;


--
-- Name: users users_last_project_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.users
    ADD CONSTRAINT users_last_project_fkey FOREIGN KEY (last_project) REFERENCES bioprotect.projects(id);


--
-- PostgreSQL database dump complete
--

\unrestrict 0b23OcroK2Hxr739hx8egrT1DZzdLdWmLFARWDryvx9Sl74XzCLHVdGLdfaSPZp

