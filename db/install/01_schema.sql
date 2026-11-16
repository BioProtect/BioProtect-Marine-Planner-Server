-- BioProtect schema, exported 2026-11-13 from bioprotect
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_raster;
CREATE EXTENSION IF NOT EXISTS h3;
--
-- PostgreSQL database dump
--

\restrict hLS8BCbBcXCNVraPFyUo4OLN9wc5eLVmGffL6PiB5yZgeteFPotOeeqvDDkOXQa

-- Dumped from database version 15.20 (Ubuntu 15.20-1.pgdg24.04+1)
-- Dumped by pg_dump version 15.20 (Ubuntu 15.20-1.pgdg24.04+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
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
-- Name: area_km2(public.geometry, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.area_km2(p_geom public.geometry, p_srid integer) RETURNS double precision
    LANGUAGE sql IMMUTABLE
    AS $$
    SELECT CASE
             WHEN p_srid IS NULL THEN ST_Area(p_geom::geography)
             ELSE ST_Area(ST_Transform(p_geom, p_srid))
           END / 1000000.0
$$;


--
-- Name: build_feature_pieces(integer, text, integer, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.build_feature_pieces(p_feature_id integer, p_feature_class text, p_from_id integer, p_to_id integer) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_repaired integer := 0;
BEGIN
    -- GEOS aborts the whole overlay with "TopologyException: ... Ring
    -- Self-intersection" on one bad ring, and shapefiles routinely carry them.
    -- ST_MakeValid can return a GeometryCollection (polygon plus dangling
    -- lines), so ST_CollectionExtract keeps just the polygonal parts.
    EXECUTE format($f$
        UPDATE bioprotect.%I
           SET geometry = ST_CollectionExtract(ST_MakeValid(geometry), 3)
         WHERE id BETWEEN $1 AND $2 AND NOT ST_IsValid(geometry)
    $f$, p_feature_class) USING p_from_id, p_to_id;
    GET DIAGNOSTICS v_repaired = ROW_COUNT;
    IF v_repaired > 0 THEN
        RAISE NOTICE 'Repaired % invalid geometr(y|ies) in %', v_repaired, p_feature_class;
    END IF;

    -- <=256-vertex pieces: without this every cell intersects the whole
    -- (often huge) polygon; a 33k-vertex feature over 18k cells took 50 s vs
    -- 1.5 s subdivided, same amounts.
    EXECUTE format($f$
        INSERT INTO bioprotect.feature_pieces (feature_unique_id, source_id, geometry)
        SELECT $3, id, ST_Subdivide(geometry, 256)
          FROM bioprotect.%I
         WHERE id BETWEEN $1 AND $2
    $f$, p_feature_class) USING p_from_id, p_to_id, p_feature_id;
END;
$_$;


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
-- Name: drop_prioritizr_input_table(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.drop_prioritizr_input_table() RETURNS trigger
    LANGUAGE plpgsql
    AS $_$
BEGIN
  IF OLD.input_table ~ '^bioprotect\.prioritizr_input_run_\d+$' THEN
    EXECUTE 'DROP TABLE IF EXISTS ' || OLD.input_table;
  END IF;
  RETURN OLD;
END;
$_$;


--
-- Name: feature_cell_batches(integer, integer, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.feature_cell_batches(p_feature_id integer, p_planning_unit_id integer, p_batch_hits integer) RETURNS TABLE(lo text, hi text)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_grid_table text;
BEGIN
    SELECT feature_class_name INTO v_grid_table
      FROM bioprotect.metadata_planning_units WHERE unique_id = p_planning_unit_id;
    IF v_grid_table IS NULL THEN
        RAISE EXCEPTION 'No planning unit table found for ID %', p_planning_unit_id;
    END IF;

    RETURN QUERY EXECUTE format($f$
        WITH hits AS (
            SELECT grid.h3_index::text AS h3, count(*) AS n
              FROM bioprotect.feature_pieces p
              JOIN bioprotect.%I grid ON grid.geometry && p.geometry
             WHERE p.feature_unique_id = $1
             GROUP BY 1
        ), numbered AS (
            SELECT h3, floor((sum(n) OVER (ORDER BY h3) - 1) / $2) AS batch FROM hits
        )
        SELECT min(h3), max(h3) FROM numbered GROUP BY batch ORDER BY batch
    $f$, v_grid_table) USING p_feature_id, p_batch_hits;
END;
$_$;


--
-- Name: feature_piece_batches(integer, text, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.feature_piece_batches(p_feature_id integer, p_feature_class text, p_batch_points integer) RETURNS TABLE(from_id integer, to_id integer)
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_max integer;
BEGIN
    EXECUTE format('SELECT max(id) FROM bioprotect.%I', p_feature_class) INTO v_max;
    IF v_max IS NULL THEN
        RETURN;
    END IF;
    IF (SELECT max(source_id) FROM bioprotect.feature_pieces
         WHERE feature_unique_id = p_feature_id) = v_max THEN
        RETURN;
    END IF;
    DELETE FROM bioprotect.feature_pieces WHERE feature_unique_id = p_feature_id;
    RETURN QUERY EXECUTE format($f$
        WITH v AS (
            SELECT id, floor((sum(ST_NPoints(geometry)) OVER (ORDER BY id)
                              - ST_NPoints(geometry)) / $1) AS batch
              FROM bioprotect.%I
        )
        SELECT min(id), max(id) FROM v GROUP BY batch ORDER BY batch
    $f$, p_feature_class) USING p_batch_points;
END;
$_$;


--
-- Name: get_planning_units_for_project(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_planning_units_for_project(p_project_id integer) RETURNS TABLE(h3_index text, cost numeric, status integer)
    LANGUAGE plpgsql STABLE
    AS $$
DECLARE
    v_active_profile_id INT;
BEGIN
    -- project_pus is filled from the grid's own h3_cells at creation, so it
    -- needs no join back to h3_cells to scope it to the grid. That join
    -- matched on LOWER(TRIM(project_area)) per row and took ~2s at 877k cells.

    -- 1. Get project's active cost profile
    SELECT active_cost_profile_id
    INTO v_active_profile_id
    FROM bioprotect.projects
    WHERE id = p_project_id;

    -- 2. If no active profile, try default cost profile for project
    IF v_active_profile_id IS NULL THEN
        SELECT id
        INTO v_active_profile_id
        FROM bioprotect.cost_profiles
        WHERE project_id = p_project_id
          AND is_default = TRUE
        ORDER BY id
        LIMIT 1;
    END IF;

    -- 3. If still no profile, fallback: cost = 0, status = 0
    IF v_active_profile_id IS NULL THEN
        RETURN QUERY
        SELECT
            pp.h3_index,
            0::NUMERIC    AS cost,
            0::INTEGER    AS status
        FROM bioprotect.project_pus pp
        WHERE pp.project_id = p_project_id;
        RETURN;
    END IF;

    -- 4. Normal path: use active (or default) cost profile
    RETURN QUERY
    SELECT
        pp.h3_index,
        cpv.cost,
        cpv.status
    FROM bioprotect.cost_profile_values cpv
    JOIN bioprotect.project_pus pp
      ON pp.id = cpv.project_pu_id
    WHERE cpv.cost_profile_id = v_active_profile_id
      AND pp.project_id = p_project_id;

END;
$$;


--
-- Name: get_planning_units_metadata(bigint); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_planning_units_metadata(planning_unit_id bigint) RETURNS TABLE(feature_class_name text, alias text, description text, creation_date text, domain text, area double precision, created_by text)
    LANGUAGE sql
    AS $_$SELECT 
  feature_class_name,
  alias, 
  description, 
  creation_date::text, 
  domain, 
  _area area, 
  created_by 
FROM bioprotect.metadata_planning_units pu 
WHERE pu.unique_id = $1::integer;
$_$;


--
-- Name: get_prioritizr_run_config(bigint); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_prioritizr_run_config(p_run_id bigint) RETURNS TABLE(run_id bigint, project_id integer, input_table text, feature_cols text[], target_prop double precision, mode text, boundary_penalty double precision, linear_cost_penalty double precision, gap double precision, time_limit_sec integer, feature_targets_json text, cohon_calibrate boolean)
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
  COALESCE((r.params->'solver'->>'gap')::double precision, 0.04) AS gap,
  COALESCE((r.params->'solver'->>'time_limit')::int, 1200) AS time_limit_sec,
  -- Build {"f_19": 0.3, "f_21": 0.5, ...} from project_features.target_value (0-100 → 0-1)
  (SELECT jsonb_object_agg(
            format('f_%s', pf.feature_unique_id),
            COALESCE(pf.target_value, 30) / 100.0
          )::text
   FROM bioprotect.project_features pf
   WHERE pf.project_id = r.project_id
  ) AS feature_targets_json,
  -- area mode: calibrate the cost penalty with Cohon's method instead of the
  -- grid-normalised default (run_prioritzr_v2.R)
  COALESCE((r.params->'penalties'->>'cohon')::boolean, false) AS cohon_calibrate
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
-- Name: get_pu_grids(); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.get_pu_grids() RETURNS TABLE(alias text, description text, creation_date text, aoi_id integer, domain text, _area double precision, envelope text, source text, created_by text, tilesetid text, planning_unit_count integer)
    LANGUAGE plpgsql
    AS $$
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
$$;


--
-- Name: grid_tiles(integer, integer, integer, json); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.grid_tiles(z integer, x integer, y integer, query_params json) RETURNS bytea
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $_$
DECLARE
  tbl text := query_params->>'layer';
  mvt bytea;
BEGIN
  -- Only registered grids: this is what stops ?layer= reading arbitrary tables
  IF NOT EXISTS (SELECT 1 FROM bioprotect.metadata_planning_units
                 WHERE feature_class_name = tbl) THEN
    RETURN NULL;
  END IF;

  EXECUTE format($q$
    SELECT ST_AsMVT(t, %L, 4096, 'geom') FROM (
      SELECT h3_index,
             ST_AsMVTGeom(ST_Transform(geometry, 3857),
                          ST_TileEnvelope($1, $2, $3), 4096, 64, true) AS geom
      FROM bioprotect.%I
      WHERE geometry && ST_Transform(ST_TileEnvelope($1, $2, $3), 4326)
    ) t WHERE geom IS NOT NULL $q$, tbl, tbl)
  INTO mvt USING z, x, y;
  RETURN mvt;
END;
$_$;


--
-- Name: insert_feature_pu_amounts(integer, integer, integer, text, text, text, text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.insert_feature_pu_amounts(p_project_id integer, p_feature_id integer, p_planning_unit_id integer, p_feature_class text, p_geom_type text, p_lo text, p_hi text) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_grid_table TEXT;
    v_area_srid  INTEGER := bioprotect.project_area_srid(p_project_id);
    v_range      TEXT := '';
BEGIN
    SELECT feature_class_name
      INTO v_grid_table
      FROM bioprotect.metadata_planning_units
     WHERE unique_id = p_planning_unit_id;

    IF v_grid_table IS NULL THEN
        RAISE EXCEPTION 'No planning unit table found for ID %', p_planning_unit_id;
    END IF;

    IF p_lo IS NOT NULL THEN
        v_range := format(' AND grid.h3_index::text BETWEEN %L AND %L', p_lo, p_hi);
    END IF;

    IF p_geom_type = 'ST_Point' THEN
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts
                (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index, SUM(feat.value)
            FROM bioprotect.%I AS grid
            JOIN bioprotect.%I AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            WHERE true %s
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_grid_table, p_feature_class, v_range);

    ELSE
        -- Polygon features: store coverage in km², measured in the project
        -- CRS (geography when that is geographic; see area_km2.sql). km² is
        -- the app-wide canonical unit for feature amounts — keeps numbers
        -- readable and avoids prioritizr's 1e6 presolve numerical cap.
        -- The union is needed: uploads can overlap (CSO has 10,920
        -- overlapping polygon pairs), so summing piece areas double-counts.
        EXECUTE format($f$
            INSERT INTO bioprotect.pu_feature_amounts (project_id, feature_unique_id, h3_index, amount)
            SELECT
                %s, %s, grid.h3_index,
                bioprotect.area_km2(
                    ST_CollectionExtract(
                        ST_Union(
                            ST_Intersection(grid.geometry, feat.geometry)),
                        3
                    ),
                    %L::integer
                ) AS amount
            FROM bioprotect.%I AS grid
            JOIN bioprotect.feature_pieces AS feat
              ON ST_Intersects(grid.geometry, feat.geometry)
            WHERE feat.feature_unique_id = %s %s
            GROUP BY grid.h3_index
        $f$, p_project_id, p_feature_id, v_area_srid, v_grid_table, p_feature_id, v_range);
    END IF;
END;
$_$;


--
-- Name: layer_tiles(integer, integer, integer, json); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.layer_tiles(z integer, x integer, y integer, query_params json) RETURNS bytea
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $_$
DECLARE
  tbl text := query_params->>'layer';
  mvt bytea;
BEGIN
  -- Only registered features and activities: this is what stops ?layer=
  -- reading arbitrary tables
  IF NOT EXISTS (SELECT 1 FROM bioprotect.metadata_interest_features
                 WHERE feature_class_name = tbl)
     AND NOT EXISTS (SELECT 1 FROM bioprotect.metadata_activities
                     WHERE activity_name = tbl) THEN
    RETURN NULL;
  END IF;

  -- ponytail: geometry only, the map uses no feature attributes. Add columns
  -- to the inner SELECT when an identify/popup needs them.
  -- ponytail: assumes EPSG:4326 data, which is what every import produces
  -- (ogr2ogr -t_srs EPSG:4326). Transform per table if that ever changes.
  -- Simplify to a quarter pixel, then clip to the tile (+ MVT buffer), THEN
  -- transform. Unsimplified, every tile transformed and clipped whole source
  -- polygons: the CSO Sublittoral Sediments layer (58k polygons, one with
  -- 496k vertices and 42k holes) took 104 s for one z4 tile -> 504 from nginx.
  -- Now the worst tile is ~2.4 s (z8). Simplify before clip: clipping that
  -- polygon at full detail alone costs ~4 s.
  -- ponytail: at high zoom the simplify barely reduces it, so the big polygon
  -- still costs ~2 s/tile there; feature_pieces could serve high zooms if needed.
  EXECUTE format($q$
    SELECT ST_AsMVT(t, %L, 4096, 'geom') FROM (
      SELECT ST_AsMVTGeom(
               ST_Transform(ST_ClipByBox2D(ST_Simplify(geometry, $4), $5), 3857),
               ST_TileEnvelope($1, $2, $3), 4096, 64, true) AS geom
      FROM bioprotect.%I
      WHERE geometry && $5
    ) t WHERE geom IS NOT NULL $q$, tbl, tbl)
  INTO mvt USING z, x, y,
    360.0 / (256 * 2 ^ z) / 4,  -- quarter of a 256px-tile pixel, in degrees
    ST_Transform(ST_TileEnvelope(z, x, y, margin => 64.0 / 4096), 4326);
  RETURN mvt;
END;
$_$;


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
      bioprotect.area_km2(hc.geometry, %L::integer) AS area_km2,
      CASE WHEN cpv.status = 1 THEN 1 ELSE 0 END AS locked_in,
      CASE WHEN cpv.status = 2 THEN 1 ELSE 0 END AS locked_out,
      %s
    FROM bioprotect.project_pus pp
    -- h3_cells keys on (h3_index, project_area, resolution): overlapping grids
    -- hold the same cell more than once. Take one geometry, or the join
    -- duplicates PUs (run 252: dup key on results) or multiplies feature SUMs.
    CROSS JOIN LATERAL (
      SELECT h.geometry FROM bioprotect.h3_cells h
       WHERE h.h3_index = pp.h3_index
       LIMIT 1
    ) hc
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
    bioprotect.project_area_srid(v_project_id),
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
-- Name: project_area_srid(integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.project_area_srid(p_project_id integer) RETURNS integer
    LANGUAGE plpgsql STABLE
    AS $$
BEGIN
    RETURN (
        SELECT CASE
                 WHEN srs.proj4text LIKE '%+units=m %' OR srs.proj4text LIKE '%+units=m'
                   THEN p.crs_epsg
               END
          FROM bioprotect.projects p
          LEFT JOIN public.spatial_ref_sys srs ON srs.srid = p.crs_epsg
         WHERE p.id = p_project_id
    );
END;
$$;


--
-- Name: reference_layer_add_shapes(integer, text, text, integer, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.reference_layer_add_shapes(p_layer_id integer, p_scratch text, p_geom_type text, p_from_id integer, p_to_id integer) RETURNS void
    LANGUAGE plpgsql
    AS $_$
DECLARE
    v_dim integer := CASE p_geom_type WHEN 'polygon' THEN 3 WHEN 'line' THEN 2 ELSE 1 END;
BEGIN
    -- Repair, keep only the layer's geometry type, one row per part, size
    -- (km² / km) worked out once per part.
    -- Every set-returning call is in FROM (LATERAL), never in a select list:
    -- there the other select-list expressions re-run for every row it returns.
    -- to_jsonb(s) (which serialises the geometry as GeoJSON before '- geometry'
    -- drops it) next to ST_Dump, and ST_Area next to ST_Subdivide, each took a
    -- 7,531-part, 496k-vertex CSO shape from seconds to about an hour.
    EXECUTE format($f$
        CREATE TEMP TABLE ref_src ON COMMIT DROP AS
        WITH src AS MATERIALIZED (
            SELECT to_jsonb(s) - 'geometry' - '_ref_id' - 'ogc_fid' AS attributes,
                   s.geometry
              FROM bioprotect.%I s
             WHERE s._ref_id BETWEEN $2 AND $3 AND s.geometry IS NOT NULL
        )
        SELECT src.attributes, d.geom,
               CASE $1 WHEN 3 THEN ST_Area(d.geom::geography) / 1e6
                       WHEN 2 THEN ST_Length(d.geom::geography) / 1e3 END AS size_km
          FROM src, LATERAL ST_Dump(ST_CollectionExtract(ST_MakeValid(src.geometry), $1)) d
    $f$, p_scratch) USING v_dim, p_from_id, p_to_id;

    IF p_geom_type = 'polygon' THEN
        INSERT INTO bioprotect.reference_layer_shapes (layer_id, part, attributes, size_km, geom)
        SELECT p_layer_id, 'fill', r.attributes, r.size_km, sd.geom
          FROM ref_src r, LATERAL ST_Subdivide(r.geom, 256) sd(geom)
        UNION ALL
        SELECT p_layer_id, 'outline', NULL, r.size_km, sd.geom
          FROM ref_src r, LATERAL ST_Subdivide(ST_Boundary(r.geom), 256) sd(geom);
    ELSIF p_geom_type = 'line' THEN
        INSERT INTO bioprotect.reference_layer_shapes (layer_id, part, attributes, size_km, geom)
        SELECT p_layer_id, 'line', r.attributes, r.size_km, sd.geom
          FROM ref_src r, LATERAL ST_Subdivide(r.geom, 256) sd(geom);
    ELSE
        INSERT INTO bioprotect.reference_layer_shapes (layer_id, part, attributes, geom)
        SELECT p_layer_id, 'point', attributes, geom FROM ref_src;
    END IF;
    DROP TABLE ref_src;
END;
$_$;


--
-- Name: reference_layer_batches(text, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.reference_layer_batches(p_scratch text, p_batch_points integer) RETURNS TABLE(from_id integer, to_id integer)
    LANGUAGE plpgsql
    AS $_$
BEGIN
    RETURN QUERY EXECUTE format($f$
        WITH v AS (
            SELECT _ref_id, floor((sum(ST_NPoints(geometry)) OVER (ORDER BY _ref_id)
                              - ST_NPoints(geometry)) / $1) AS batch
              FROM bioprotect.%I
        )
        SELECT min(_ref_id), max(_ref_id) FROM v GROUP BY batch ORDER BY batch
    $f$, p_scratch) USING p_batch_points;
END;
$_$;


--
-- Name: reference_tiles(integer, integer, integer, json); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.reference_tiles(z integer, x integer, y integer, query_params json) RETURNS bytea
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $$
DECLARE
  v_layer   integer;
  env       geometry := ST_TileEnvelope(z, x, y);
  env4326   geometry := ST_Transform(ST_TileEnvelope(z, x, y), 4326);
  px_deg    double precision := 360.0 / (256 * 2 ^ z);
  px_km     double precision := 40075.0 / (256 * 2 ^ z);
  out_mvt   bytea := '';
  part_mvt  bytea;
  p         text;
BEGIN
  BEGIN
    v_layer := (query_params->>'layer')::integer;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
  IF v_layer IS NULL THEN
    RETURN NULL;
  END IF;

  FOREACH p IN ARRAY ARRAY['fill', 'outline', 'line', 'point'] LOOP
    SELECT ST_AsMVT(t, 'ref_' || p, 4096, 'geom') INTO part_mvt FROM (
      SELECT s.attributes::text AS attrs,
             ST_AsMVTGeom(
               ST_Transform(CASE WHEN p = 'point' THEN s.geom
                                 ELSE ST_Simplify(s.geom, px_deg / 4) END, 3857),
               env, 4096, 64, true) AS geom
        FROM bioprotect.reference_layer_shapes s
       WHERE s.layer_id = v_layer AND s.part = p AND s.geom && env4326
         AND (p = 'point'
              OR (p IN ('fill', 'outline') AND s.size_km >= px_km ^ 2)
              OR (p = 'line' AND s.size_km >= px_km))
    ) t WHERE geom IS NOT NULL;
    out_mvt := out_mvt || coalesce(part_mvt, '');
  END LOOP;

  -- The skipped sub-pixel polygons and lines come back as dots (ref_dot), so
  -- a layer of small shapes is still visible zoomed out.
  -- ponytail: one dot per 2 px cell (snap 32 of 4096), the other shapes in
  -- that cell lose their click attributes until you zoom in
  SELECT ST_AsMVT(t, 'ref_dot', 4096, 'geom') INTO part_mvt FROM (
    SELECT DISTINCT ON (ST_AsBinary(geom)) attrs, geom FROM (
      SELECT s.attributes::text AS attrs,
             ST_SnapToGrid(ST_AsMVTGeom(
               ST_Transform(ST_PointOnSurface(s.geom), 3857),
               env, 4096, 0, true), 32) AS geom
        FROM bioprotect.reference_layer_shapes s
       WHERE s.layer_id = v_layer AND s.geom && env4326
         AND ((s.part = 'fill' AND s.size_km < px_km ^ 2)
              OR (s.part = 'line' AND s.size_km < px_km))
    ) q WHERE geom IS NOT NULL
  ) t;
  out_mvt := out_mvt || coalesce(part_mvt, '');
  RETURN out_mvt;
END;
$$;


--
-- Name: run_cumulative_impact(integer, integer[], text, text, text, numeric); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.run_cumulative_impact(_project_id integer, _activity_ids integer[], _profile_name text, _description text DEFAULT ''::text, _user text DEFAULT 'system'::text, _floor numeric DEFAULT 0.001) RETURNS integer
    LANGUAGE plpgsql
    AS $_$
DECLARE
    _cost_profile INTEGER;
    _act          RECORD;
BEGIN
    -- ---------------------------------------------------------------
    -- Validate floor: cells must never have cost <= 0 because
    -- Prioritizr would always favour them.
    -- ---------------------------------------------------------------
    IF _floor IS NULL OR _floor <= 0 OR _floor >= 1 THEN
        RAISE EXCEPTION 'floor must be strictly between 0 and 1';
    END IF;

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
    -- Record which activities make up this cost profile so the UI can
    -- list and visualise them later.
    -- ---------------------------------------------------------------
    INSERT INTO bioprotect.cost_profile_activities (cost_profile_id, activity_id)
    SELECT _cost_profile, aid
      FROM unnest(_activity_ids) AS aid
     WHERE EXISTS (
         SELECT 1 FROM bioprotect.metadata_activities ma WHERE ma.id = aid
     );

    -- ---------------------------------------------------------------
    -- Compute cumulative impact per hex and insert as cost values.
    --
    -- A hex is "covered" by an activity when its centroid lies in any of
    -- the activity's own polygons, tested through that table's spatial
    -- index - no union, no simplify, so huge activities cost no more than
    -- the cells they touch. Each covered (hex, activity) then fans out to
    -- the activity's pressures straight from PAD (rppscore > 0), so PAD
    -- edits apply to the next run. (This replaced a `pressures` table that
    -- held a merged copy of the footprint per PAD row; dropped in 0018.)
    --
    -- Costs are rescaled into [_floor, 1] so no hex ever stores 0.
    -- ---------------------------------------------------------------
    DROP TABLE IF EXISTS pg_temp.ci_hexes, pg_temp.ci_coverage;
    CREATE TEMP TABLE ci_hexes ON COMMIT DROP AS
    SELECT pp.id AS project_pu_id,
           pp.h3_index,
           ST_Centroid(hc.geometry) AS hex_centroid
      FROM bioprotect.project_pus pp
      -- One geometry per cell: overlapping grids repeat h3_index in
      -- h3_cells, which would multiply every hex's impact.
      CROSS JOIN LATERAL (
        SELECT h.geometry FROM bioprotect.h3_cells h
         WHERE h.h3_index = pp.h3_index
         LIMIT 1
      ) hc
     WHERE pp.project_id = _project_id;
    CREATE INDEX ON ci_hexes USING gist (hex_centroid);
    ANALYZE ci_hexes;

    CREATE TEMP TABLE ci_coverage (
        project_pu_id INTEGER,
        h3_index      TEXT,
        activity_id   INTEGER
    ) ON COMMIT DROP;

    -- Driven from the activity's polygons into the indexed centroids, so
    -- each (possibly 100k-vertex) polygon is prepared once and probed many
    -- times. Hex-driven EXISTS was 60x slower on a 192k-cell grid.
    -- ST_Intersects, not ST_Contains: activities come as several polygons
    -- (raster pieces, shapefile parts), and a centroid on a shared edge is
    -- inside the activity even though no single polygon "contains" it.
    FOR _act IN
        SELECT ma.id, ma.activity_name
          FROM bioprotect.metadata_activities ma
         WHERE ma.id = ANY(_activity_ids)
           AND ma.activity_name IS NOT NULL
    LOOP
        EXECUTE format($f$
            INSERT INTO ci_coverage (project_pu_id, h3_index, activity_id)
            SELECT DISTINCT h.project_pu_id, h.h3_index, %s
              FROM bioprotect.%I a
              JOIN ci_hexes h ON ST_Intersects(a.geometry, h.hex_centroid)
        $f$, _act.id, _act.activity_name);
    END LOOP;

    WITH hex_pressures AS (
        SELECT cov.project_pu_id,
               cov.h3_index,
               pad.pressuretitle,
               pad.rppscore
          FROM ci_coverage cov
          JOIN bioprotect.metadata_activities ma ON ma.id = cov.activity_id
          JOIN bioprotect.pad pad
            ON pad.activitytitle = ma.activity
           AND pad.rppscore > 0
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
                   hp.rppscore
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
    -- Insert impacted hexes with normalised cost in [_floor, 1].
    -- Linear remap from [0, 1] into [_floor, 1] so the lowest impact
    -- still lands on _floor, never 0.
    INSERT INTO bioprotect.cost_profile_values
        (cost_profile_id, project_pu_id, cost, status)
    SELECT _cost_profile,
           c.project_pu_id,
           CASE
             WHEN max_impact.val > 0
             THEN GREATEST(
                    _floor,
                    LEAST(
                        1.0,
                        _floor + (1.0 - _floor) * (c.impact / max_impact.val)
                    )
                  )
             ELSE _floor
           END,
           0
      FROM cumulative c,
           (SELECT MAX(impact) AS val FROM cumulative) max_impact;

    -- Fill non-impacted hexes with the floor cost (not zero).
    INSERT INTO bioprotect.cost_profile_values
        (cost_profile_id, project_pu_id, cost, status)
    SELECT _cost_profile, pp.id, _floor, 0
      FROM bioprotect.project_pus pp
     WHERE pp.project_id = _project_id
       AND NOT EXISTS (
           SELECT 1 FROM bioprotect.cost_profile_values cpv
            WHERE cpv.cost_profile_id = _cost_profile
              AND cpv.project_pu_id   = pp.id
       );

    -- The cost layer is read straight after this; without fresh statistics
    -- the planner assumes the new profile has ~1 row and picks a quadratic join.
    ANALYZE bioprotect.cost_profile_values;

    RETURN _cost_profile;
END;
$_$;


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
BEGIN
    -- NULL target = not set yet (a feature just added to the project). Stored
    -- as NULL so get_prioritizr_run_config falls back to its 30% default; on
    -- update the COALESCE below keeps any target already set.
    -- 3) Upsert into project_features
    INSERT INTO bioprotect.project_features (
        project_id, feature_unique_id, target_type, target_value, spf, weight
    )
    VALUES (p_project_id, p_feature_id, p_target_type, p_target_value, p_spf, p_weight)
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
-- Name: wdpa_publish(text); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.wdpa_publish(p_release text) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_areas bigint;
BEGIN
    DROP TABLE IF EXISTS bioprotect.wdpa_areas_new;
    CREATE TABLE bioprotect.wdpa_areas_new (LIKE bioprotect.wdpa_areas INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

    -- MATERIALIZED stops the planner inlining these: inlined, MakeValid and the
    -- area ran once per dumped part on the whole multipolygon (quadratic; the
    -- Oct2026 load ran for hours on Alaska Maritime's 20k parts).
    WITH valid AS MATERIALIZED (
        SELECT s.wdpaid::bigint AS wdpaid, s.name, s.desig_eng, s.iucn_cat,
               s.status, s.status_yr::integer AS status_yr,
               ST_CollectionExtract(ST_MakeValid(s.geom), 3) AS geom
        FROM bioprotect.wdpa_staging s
    ), area AS MATERIALIZED (
        SELECT v.*, ST_Area(v.geom::geography) / 1e6 AS area_km2
        FROM valid v
        WHERE NOT ST_IsEmpty(v.geom)
    ), poly AS MATERIALIZED (
        SELECT a.wdpaid, a.name, a.desig_eng, a.iucn_cat, a.status, a.status_yr,
               a.area_km2, d.geom AS poly
        FROM area a, ST_Dump(a.geom) d
    )
    INSERT INTO bioprotect.wdpa_areas_new
        (wdpaid, name, desig_eng, iucn_cat, status, status_yr, area_km2, part, geom)
    SELECT wdpaid, name, desig_eng, iucn_cat, status, status_yr, area_km2, 'fill', p.geom
    FROM poly, ST_Subdivide(poly, 256) s(geom), ST_Dump(s.geom) p
    WHERE GeometryType(p.geom) = 'POLYGON'
    UNION ALL
    SELECT wdpaid, NULL, NULL, NULL, NULL, NULL, area_km2, 'line', p.geom
    FROM poly, ST_Subdivide(ST_Boundary(poly), 256) s(geom), ST_Dump(s.geom) p
    WHERE GeometryType(p.geom) = 'LINESTRING';

    CREATE INDEX wdpa_areas_new_geom_gist ON bioprotect.wdpa_areas_new USING gist (geom);
    ANALYZE bioprotect.wdpa_areas_new;

    -- the swap
    DROP TABLE bioprotect.wdpa_areas;
    ALTER TABLE bioprotect.wdpa_areas_new RENAME TO wdpa_areas;
    ALTER INDEX bioprotect.wdpa_areas_new_geom_gist RENAME TO wdpa_areas_geom_gist;
    DELETE FROM bioprotect.wdpa_release;
    INSERT INTO bioprotect.wdpa_release (release) VALUES (p_release);
    DROP TABLE bioprotect.wdpa_staging;

    SELECT count(DISTINCT wdpaid) INTO v_areas FROM bioprotect.wdpa_areas;
    RETURN v_areas;
END;
$$;


--
-- Name: wdpa_tiles(integer, integer, integer); Type: FUNCTION; Schema: bioprotect; Owner: -
--

CREATE FUNCTION bioprotect.wdpa_tiles(z integer, x integer, y integer) RETURNS bytea
    LANGUAGE plpgsql STABLE PARALLEL SAFE
    AS $$
DECLARE
  env       geometry := ST_TileEnvelope(z, x, y);
  env4326   geometry := ST_Transform(ST_TileEnvelope(z, x, y), 4326);
  -- one 256px-tile pixel: degrees (for simplifying) and km² (for culling)
  px_deg    double precision := 360.0 / (256 * 2 ^ z);
  px_km2    double precision := (40075.0 / (256 * 2 ^ z)) ^ 2;
  fill_mvt  bytea;
  line_mvt  bytea;
BEGIN
  SELECT ST_AsMVT(t, 'wdpa_fill', 4096, 'geom') INTO fill_mvt FROM (
    SELECT wdpaid, name, desig_eng, iucn_cat, status, status_yr,
           ST_AsMVTGeom(ST_Transform(ST_Simplify(geom, px_deg / 4), 3857),
                        env, 4096, 64, true) AS geom
    FROM bioprotect.wdpa_areas
    WHERE part = 'fill' AND geom && env4326 AND area_km2 >= px_km2
  ) t WHERE geom IS NOT NULL;

  SELECT ST_AsMVT(t, 'wdpa_line', 4096, 'geom') INTO line_mvt FROM (
    SELECT wdpaid,
           ST_AsMVTGeom(ST_Transform(ST_Simplify(geom, px_deg / 4), 3857),
                        env, 4096, 64, true) AS geom
    FROM bioprotect.wdpa_areas
    WHERE part = 'line' AND geom && env4326 AND area_km2 >= px_km2
  ) t WHERE geom IS NOT NULL;

  RETURN coalesce(fill_mvt, '') || coalesce(line_mvt, '');
END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: cost_profile_activities; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.cost_profile_activities (
    cost_profile_id integer NOT NULL,
    activity_id integer NOT NULL
);


--
-- Name: cost_profile_values; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.cost_profile_values (
    id integer NOT NULL,
    cost_profile_id integer NOT NULL,
    project_pu_id integer NOT NULL,
    cost numeric NOT NULL,
    status integer,
    CONSTRAINT cost_profile_values_cost_positive CHECK ((cost > (0)::numeric))
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
    is_default boolean DEFAULT false,
    source_raster_id integer
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
-- Name: cost_rasters; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.cost_rasters (
    id integer NOT NULL,
    name text NOT NULL,
    description text DEFAULT ''::text,
    source_filename text,
    band integer DEFAULT 1 NOT NULL,
    stat text DEFAULT 'mean'::text NOT NULL,
    checksum text,
    bounds public.geometry(Polygon,4326),
    owner_id integer,
    visibility text DEFAULT 'private'::text NOT NULL,
    hex_count integer DEFAULT 0 NOT NULL,
    date_created timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT cost_rasters_visibility_check CHECK ((visibility = ANY (ARRAY['private'::text, 'shared'::text])))
);


--
-- Name: cost_rasters_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.cost_rasters_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: cost_rasters_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.cost_rasters_id_seq OWNED BY bioprotect.cost_rasters.id;


--
-- Name: feature_pieces; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.feature_pieces (
    feature_unique_id integer NOT NULL,
    source_id integer NOT NULL,
    geometry public.geometry NOT NULL
);


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
-- Name: gaul_2015_simplified_1km_ogc_fid_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.gaul_2015_simplified_1km_ogc_fid_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


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
    geometry public.geometry(Polygon,4326)
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
-- Name: land; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.land (
    id bigint NOT NULL,
    geom public.geometry(Polygon,4326) NOT NULL
);


--
-- Name: land_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.land_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: land_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.land_id_seq OWNED BY bioprotect.land.id;


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
    active_cost_profile_id integer,
    crs_epsg integer DEFAULT 4326 NOT NULL
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
-- Name: pu_h3; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.pu_h3 (
    pu_id integer NOT NULL,
    h3_index text NOT NULL,
    resolution integer NOT NULL
);


--
-- Name: raster_hex_values; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.raster_hex_values (
    raster_id integer NOT NULL,
    h3_index text NOT NULL,
    value double precision NOT NULL
);


--
-- Name: reference_layer_shapes; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.reference_layer_shapes (
    layer_id integer NOT NULL,
    part text NOT NULL,
    attributes jsonb,
    size_km double precision DEFAULT 0 NOT NULL,
    geom public.geometry(Geometry,4326) NOT NULL,
    CONSTRAINT reference_layer_shapes_part_check CHECK ((part = ANY (ARRAY['fill'::text, 'outline'::text, 'line'::text, 'point'::text])))
);


--
-- Name: reference_layers; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.reference_layers (
    id integer NOT NULL,
    name text NOT NULL,
    description text,
    geom_type text NOT NULL,
    colour text NOT NULL,
    created_by text,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT reference_layers_geom_type_check CHECK ((geom_type = ANY (ARRAY['polygon'::text, 'line'::text, 'point'::text])))
);


--
-- Name: reference_layers_id_seq; Type: SEQUENCE; Schema: bioprotect; Owner: -
--

CREATE SEQUENCE bioprotect.reference_layers_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: reference_layers_id_seq; Type: SEQUENCE OWNED BY; Schema: bioprotect; Owner: -
--

ALTER SEQUENCE bioprotect.reference_layers_id_seq OWNED BY bioprotect.reference_layers.id;


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
-- Name: wdpa_areas; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.wdpa_areas (
    wdpaid bigint NOT NULL,
    name text,
    desig_eng text,
    iucn_cat text,
    status text,
    status_yr integer,
    area_km2 double precision NOT NULL,
    part text NOT NULL,
    geom public.geometry(Geometry,4326) NOT NULL,
    CONSTRAINT wdpa_areas_part_check CHECK ((part = ANY (ARRAY['fill'::text, 'line'::text])))
);


--
-- Name: wdpa_release; Type: TABLE; Schema: bioprotect; Owner: -
--

CREATE TABLE bioprotect.wdpa_release (
    release text NOT NULL,
    loaded_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: cost_profile_values id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_values ALTER COLUMN id SET DEFAULT nextval('bioprotect.cost_profile_values_id_seq'::regclass);


--
-- Name: cost_profiles id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profiles ALTER COLUMN id SET DEFAULT nextval('bioprotect.cost_profiles_id_seq'::regclass);


--
-- Name: cost_rasters id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_rasters ALTER COLUMN id SET DEFAULT nextval('bioprotect.cost_rasters_id_seq'::regclass);


--
-- Name: jncc_sensitivities id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.jncc_sensitivities ALTER COLUMN id SET DEFAULT nextval('bioprotect.jncc_sensitivities_id_seq'::regclass);


--
-- Name: land id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.land ALTER COLUMN id SET DEFAULT nextval('bioprotect.land_id_seq'::regclass);


--
-- Name: metadata_activities id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_activities ALTER COLUMN id SET DEFAULT nextval('bioprotect.metadata_activities_id_seq'::regclass);


--
-- Name: metadata_interest_features unique_id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_interest_features ALTER COLUMN unique_id SET DEFAULT nextval('bioprotect.metadata_interest_features_unique_id_seq'::regclass);


--
-- Name: metadata_planning_units unique_id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.metadata_planning_units ALTER COLUMN unique_id SET DEFAULT nextval('bioprotect.metadata_planning_units_unique_id_seq'::regclass);


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
-- Name: reference_layers id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.reference_layers ALTER COLUMN id SET DEFAULT nextval('bioprotect.reference_layers_id_seq'::regclass);


--
-- Name: sensitivity_matrix id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.sensitivity_matrix ALTER COLUMN id SET DEFAULT nextval('bioprotect.sensitivity_matrix_id_seq'::regclass);


--
-- Name: users id; Type: DEFAULT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.users ALTER COLUMN id SET DEFAULT nextval('bioprotect.users_id_seq'::regclass);


--
-- Name: cost_profile_activities cost_profile_activities_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_activities
    ADD CONSTRAINT cost_profile_activities_pkey PRIMARY KEY (cost_profile_id, activity_id);


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
-- Name: cost_rasters cost_rasters_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_rasters
    ADD CONSTRAINT cost_rasters_pkey PRIMARY KEY (id);


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
-- Name: jncc_sensitivities jncc_sensitivities_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.jncc_sensitivities
    ADD CONSTRAINT jncc_sensitivities_pkey PRIMARY KEY (id);


--
-- Name: land land_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.land
    ADD CONSTRAINT land_pkey PRIMARY KEY (id);


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
-- Name: raster_hex_values raster_hex_values_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.raster_hex_values
    ADD CONSTRAINT raster_hex_values_pkey PRIMARY KEY (raster_id, h3_index);


--
-- Name: reference_layers reference_layers_pkey; Type: CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.reference_layers
    ADD CONSTRAINT reference_layers_pkey PRIMARY KEY (id);


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
-- Name: feature_pieces_feature_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX feature_pieces_feature_idx ON bioprotect.feature_pieces USING btree (feature_unique_id, source_id);


--
-- Name: feature_pieces_geometry_gix; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX feature_pieces_geometry_gix ON bioprotect.feature_pieces USING gist (geometry);


--
-- Name: h3_cells_geometry_gix; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX h3_cells_geometry_gix ON bioprotect.h3_cells USING gist (geometry);


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
-- Name: idx_cost_profile_activities_activity; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_cost_profile_activities_activity ON bioprotect.cost_profile_activities USING btree (activity_id);


--
-- Name: idx_cost_profile_values_project_pu; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_cost_profile_values_project_pu ON bioprotect.cost_profile_values USING btree (project_pu_id);


--
-- Name: idx_cost_rasters_bounds; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_cost_rasters_bounds ON bioprotect.cost_rasters USING gist (bounds);


--
-- Name: idx_cost_rasters_owner; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_cost_rasters_owner ON bioprotect.cost_rasters USING btree (owner_id);


--
-- Name: idx_cost_rasters_owner_checksum; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE UNIQUE INDEX idx_cost_rasters_owner_checksum ON bioprotect.cost_rasters USING btree (owner_id, checksum, band, stat) WHERE (checksum IS NOT NULL);


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
-- Name: idx_raster_hex_values_h3; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX idx_raster_hex_values_h3 ON bioprotect.raster_hex_values USING btree (h3_index);


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
-- Name: ix_bioprotect_pad_index; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX ix_bioprotect_pad_index ON bioprotect.pad USING btree (index);


--
-- Name: land_geom_gist; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX land_geom_gist ON bioprotect.land USING gist (geom);


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
-- Name: reference_layer_shapes_geom_gix; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX reference_layer_shapes_geom_gix ON bioprotect.reference_layer_shapes USING gist (geom);


--
-- Name: reference_layer_shapes_layer_idx; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX reference_layer_shapes_layer_idx ON bioprotect.reference_layer_shapes USING btree (layer_id);


--
-- Name: reference_layers_name_key; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE UNIQUE INDEX reference_layers_name_key ON bioprotect.reference_layers USING btree (lower(name));


--
-- Name: wdpa_areas_geom_gist; Type: INDEX; Schema: bioprotect; Owner: -
--

CREATE INDEX wdpa_areas_geom_gist ON bioprotect.wdpa_areas USING gist (geom);


--
-- Name: prioritizr_runs trg_drop_prioritizr_input_table; Type: TRIGGER; Schema: bioprotect; Owner: -
--

CREATE TRIGGER trg_drop_prioritizr_input_table AFTER DELETE ON bioprotect.prioritizr_runs FOR EACH ROW EXECUTE FUNCTION bioprotect.drop_prioritizr_input_table();


--
-- Name: project_features trg_pf_updated; Type: TRIGGER; Schema: bioprotect; Owner: -
--

CREATE TRIGGER trg_pf_updated BEFORE UPDATE ON bioprotect.project_features FOR EACH ROW EXECUTE FUNCTION bioprotect.touch_updated_at();


--
-- Name: cost_profile_activities cost_profile_activities_activity_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_activities
    ADD CONSTRAINT cost_profile_activities_activity_id_fkey FOREIGN KEY (activity_id) REFERENCES bioprotect.metadata_activities(id) ON DELETE CASCADE;


--
-- Name: cost_profile_activities cost_profile_activities_cost_profile_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profile_activities
    ADD CONSTRAINT cost_profile_activities_cost_profile_id_fkey FOREIGN KEY (cost_profile_id) REFERENCES bioprotect.cost_profiles(id) ON DELETE CASCADE;


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
-- Name: cost_profiles cost_profiles_source_raster_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_profiles
    ADD CONSTRAINT cost_profiles_source_raster_id_fkey FOREIGN KEY (source_raster_id) REFERENCES bioprotect.cost_rasters(id) ON DELETE SET NULL;


--
-- Name: cost_rasters cost_rasters_owner_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.cost_rasters
    ADD CONSTRAINT cost_rasters_owner_id_fkey FOREIGN KEY (owner_id) REFERENCES bioprotect.users(id) ON DELETE SET NULL;


--
-- Name: feature_pieces feature_pieces_feature_unique_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.feature_pieces
    ADD CONSTRAINT feature_pieces_feature_unique_id_fkey FOREIGN KEY (feature_unique_id) REFERENCES bioprotect.metadata_interest_features(unique_id) ON DELETE CASCADE;


--
-- Name: projects fk_planning_unit; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.projects
    ADD CONSTRAINT fk_planning_unit FOREIGN KEY (planning_unit_id) REFERENCES bioprotect.metadata_planning_units(unique_id) ON DELETE SET NULL;


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
-- Name: raster_hex_values raster_hex_values_raster_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.raster_hex_values
    ADD CONSTRAINT raster_hex_values_raster_id_fkey FOREIGN KEY (raster_id) REFERENCES bioprotect.cost_rasters(id) ON DELETE CASCADE;


--
-- Name: reference_layer_shapes reference_layer_shapes_layer_id_fkey; Type: FK CONSTRAINT; Schema: bioprotect; Owner: -
--

ALTER TABLE ONLY bioprotect.reference_layer_shapes
    ADD CONSTRAINT reference_layer_shapes_layer_id_fkey FOREIGN KEY (layer_id) REFERENCES bioprotect.reference_layers(id) ON DELETE CASCADE;


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

\unrestrict hLS8BCbBcXCNVraPFyUo4OLN9wc5eLVmGffL6PiB5yZgeteFPotOeeqvDDkOXQa

