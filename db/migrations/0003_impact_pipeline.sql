-- Migration 0003: Impact pipeline — new tables, indexes, and functions
-- Created: 2026-04-21
--
-- Adds:
--   Tables  : grid_boundary_edges, pressures, sensitivity_matrix
--   Indexes : on all three new tables
--   Functions (new)     : create_pressures_from_activity, get_project_boundary_edges,
--                         populate_grid_boundary_edges, run_cumulative_impact,
--                         run_impact_pipeline
--   Functions (updated) : get_prioritizr_run_config  (+feature_targets_json column)
--                         set_active_profile_pu_statuses (+p_status3_h3 parameter)
-- ============================================================


-- ============================================================
-- 1. grid_boundary_edges
-- ============================================================
CREATE TABLE IF NOT EXISTS bioprotect.grid_boundary_edges (
    planning_unit_id integer      NOT NULL,
    h3_a             text         NOT NULL,
    h3_b             text         NOT NULL,
    boundary         double precision NOT NULL DEFAULT 1.0
);

ALTER TABLE bioprotect.grid_boundary_edges
    DROP CONSTRAINT IF EXISTS grid_boundary_edges_pkey;
ALTER TABLE bioprotect.grid_boundary_edges
    ADD  CONSTRAINT grid_boundary_edges_pkey PRIMARY KEY (planning_unit_id, h3_a, h3_b);

CREATE INDEX IF NOT EXISTS idx_grid_boundary_edges_grid
    ON bioprotect.grid_boundary_edges USING btree (planning_unit_id);


-- ============================================================
-- 2. pressures
--    Old schema had raster_data + srid columns; new schema uses
--    a vector geometry column. Pressures are computed data so it
--    is safe to drop and recreate the table.
-- ============================================================
DROP TABLE IF EXISTS bioprotect.pressures CASCADE;

CREATE TABLE bioprotect.pressures (
    id            SERIAL PRIMARY KEY,
    activity_id   integer,
    pressuretitle text    NOT NULL,
    rppscore      numeric NOT NULL,
    geometry      public.geometry(Geometry, 4326) NOT NULL,
    created_at    timestamp without time zone DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_pressures_activity_id
    ON bioprotect.pressures USING btree (activity_id);
CREATE INDEX IF NOT EXISTS idx_pressures_geom
    ON bioprotect.pressures USING gist (geometry);
CREATE INDEX IF NOT EXISTS idx_pressures_pressuretitle
    ON bioprotect.pressures USING btree (pressuretitle);


-- ============================================================
-- 3. sensitivity_matrix
-- ============================================================
CREATE TABLE IF NOT EXISTS bioprotect.sensitivity_matrix (
    id                SERIAL PRIMARY KEY,
    eunis_code        text NOT NULL,
    pressure          text NOT NULL,
    sensitivity_score numeric NOT NULL,
    CONSTRAINT sensitivity_matrix_eunis_code_pressure_key UNIQUE (eunis_code, pressure)
);

CREATE INDEX IF NOT EXISTS idx_sensitivity_eunis
    ON bioprotect.sensitivity_matrix USING btree (eunis_code);
CREATE INDEX IF NOT EXISTS idx_sensitivity_pressure
    ON bioprotect.sensitivity_matrix USING btree (pressure);


-- ============================================================
-- 4. Updated function: get_prioritizr_run_config
--    (adds feature_targets_json output column)
-- ============================================================
CREATE OR REPLACE FUNCTION bioprotect.get_prioritizr_run_config(p_run_id bigint)
RETURNS TABLE(
    run_id              bigint,
    project_id          integer,
    input_table         text,
    feature_cols        text[],
    target_prop         double precision,
    mode                text,
    boundary_penalty    double precision,
    linear_cost_penalty double precision,
    gap                 double precision,
    time_limit_sec      integer,
    feature_targets_json text
)
LANGUAGE sql STABLE AS $$
SELECT
  r.id AS run_id,
  r.project_id,
  r.input_table,
  r.feature_cols,
  COALESCE((r.params->'targets'->>'prop')::double precision, 0.30) AS target_prop,
  COALESCE((r.params->>'mode')::text, 'area')                       AS mode,
  COALESCE((r.params->'penalties'->>'boundary')::double precision, 0.0) AS boundary_penalty,
  COALESCE((r.params->'penalties'->>'linear')::double precision, 0.1)   AS linear_cost_penalty,
  COALESCE((r.params->'solver'->>'gap')::double precision, 0.10)        AS gap,
  COALESCE((r.params->'solver'->>'time_limit')::int, 1200)              AS time_limit_sec,
  -- Per-feature targets: {"f_19": 0.3, "f_21": 0.5, ...}  (target_value 0-100 → 0-1)
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


-- ============================================================
-- 5. Updated function: set_active_profile_pu_statuses
--    (adds optional p_status3_h3 parameter)
-- ============================================================
CREATE OR REPLACE FUNCTION bioprotect.set_active_profile_pu_statuses(
    p_project_id  integer,
    p_status1_h3  text[]  DEFAULT ARRAY[]::text[],
    p_status2_h3  text[]  DEFAULT ARRAY[]::text[],
    p_status3_h3  text[]  DEFAULT ARRAY[]::text[]
)
RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_profile_id INT;
BEGIN
    SELECT active_cost_profile_id
      INTO v_profile_id
      FROM bioprotect.projects
     WHERE id = p_project_id;

    IF v_profile_id IS NULL THEN
        RAISE EXCEPTION 'Project % has no active_cost_profile_id', p_project_id;
    END IF;

    -- Single-pass update using CASE
    UPDATE bioprotect.cost_profile_values cpv
       SET status = CASE
                       WHEN pp.h3_index = ANY(p_status1_h3) THEN 1
                       WHEN pp.h3_index = ANY(p_status2_h3) THEN 2
                       WHEN pp.h3_index = ANY(p_status3_h3) THEN 3
                       ELSE 0
                   END
      FROM bioprotect.project_pus pp
     WHERE cpv.cost_profile_id = v_profile_id
       AND cpv.project_pu_id   = pp.id
       AND pp.project_id       = p_project_id;
END;
$$;


-- ============================================================
-- 6. New function: create_pressures_from_activity
-- ============================================================
CREATE OR REPLACE FUNCTION bioprotect.create_pressures_from_activity(
    _activity_id integer
)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    _activity_title TEXT;
    _activity_table TEXT;
    _activity_geom  geometry;
    _count          INTEGER;
BEGIN
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

    -- Re-runnable: clear existing pressures for this activity first
    DELETE FROM bioprotect.pressures WHERE activity_id = _activity_id;

    -- One pressure row per PAD entry for this activity
    INSERT INTO bioprotect.pressures (activity_id, pressuretitle, rppscore, geometry)
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


-- ============================================================
-- 7. New function: get_project_boundary_edges
-- ============================================================
CREATE OR REPLACE FUNCTION bioprotect.get_project_boundary_edges(
    p_project_id integer
)
RETURNS TABLE(pu_id text, nbr_id text, boundary double precision)
LANGUAGE sql STABLE AS $$
  SELECT gbe.h3_a AS pu_id,
         gbe.h3_b AS nbr_id,
         gbe.boundary
    FROM bioprotect.grid_boundary_edges gbe
    JOIN bioprotect.projects pr ON pr.planning_unit_id = gbe.planning_unit_id
   WHERE pr.id = p_project_id;
$$;


-- ============================================================
-- 8. New function: populate_grid_boundary_edges
-- ============================================================
CREATE OR REPLACE FUNCTION bioprotect.populate_grid_boundary_edges(
    p_planning_unit_id integer
)
RETURNS bigint
LANGUAGE plpgsql AS $$
DECLARE
  v_count BIGINT;
BEGIN
  -- Clear any existing edges for this grid (re-runnable)
  DELETE FROM bioprotect.grid_boundary_edges
   WHERE planning_unit_id = p_planning_unit_id;

  -- Compute neighbors via hash join — much faster than correlated EXISTS
  -- for large grids (100K+ planning units).
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


-- ============================================================
-- 9. New function: run_cumulative_impact
-- ============================================================
CREATE OR REPLACE FUNCTION bioprotect.run_cumulative_impact(
    _project_id   integer,
    _activity_ids integer[],
    _profile_name text,
    _description  text    DEFAULT ''::text,
    _user         text    DEFAULT 'system'::text
)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    _cost_profile INTEGER;
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM bioprotect.project_pus WHERE project_id = _project_id
    ) THEN
        RAISE EXCEPTION 'Project % has no planning units', _project_id;
    END IF;

    INSERT INTO bioprotect.cost_profiles (project_id, name, description, created_by, is_default)
    VALUES (_project_id, _profile_name, _description, _user, false)
    RETURNING id INTO _cost_profile;

    -- Hex centroid containment (not full polygon intersection) for performance.
    -- A hex is "covered" if its centroid falls within the activity geometry.
    WITH project_hexes AS (
        SELECT pp.id   AS project_pu_id,
               pp.h3_index,
               ST_Centroid(hc.geometry) AS hex_centroid
          FROM bioprotect.project_pus pp
          JOIN bioprotect.h3_cells hc ON hc.h3_index = pp.h3_index
         WHERE pp.project_id = _project_id
    ),
    activity_geoms AS (
        SELECT DISTINCT ON (activity_id)
               activity_id,
               ST_Simplify(geometry, 0.0001) AS geometry
          FROM bioprotect.pressures
         WHERE activity_id = ANY(_activity_ids)
    ),
    hex_activity_coverage AS (
        SELECT ph.project_pu_id,
               ph.h3_index,
               ag.activity_id,
               1::numeric AS pressure_coverage
          FROM project_hexes ph
          JOIN activity_geoms ag ON ST_Contains(ag.geometry, ph.hex_centroid)
    ),
    hex_pressures AS (
        SELECT hac.project_pu_id,
               hac.h3_index,
               p.pressuretitle,
               p.rppscore,
               hac.pressure_coverage
          FROM hex_activity_coverage hac
          JOIN bioprotect.pressures p ON p.activity_id = hac.activity_id
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
          JOIN hex_features hf ON hp.h3_index = hf.h3_index
          LEFT JOIN bioprotect.sensitivity_matrix sm
            ON sm.eunis_code = hf.feature_name
           AND sm.pressure   = hp.pressuretitle
         GROUP BY hp.project_pu_id
    )
    INSERT INTO bioprotect.cost_profile_values (cost_profile_id, project_pu_id, cost, status)
    SELECT _cost_profile,
           c.project_pu_id,
           CASE
             WHEN max_impact.val > 0 THEN c.impact / max_impact.val
             ELSE 0
           END,
           0
      FROM cumulative c,
           (SELECT MAX(impact) AS val FROM cumulative) max_impact;

    -- Zero cost for hexes with no impact
    INSERT INTO bioprotect.cost_profile_values (cost_profile_id, project_pu_id, cost, status)
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


-- ============================================================
-- 10. New function: run_impact_pipeline
-- ============================================================
CREATE OR REPLACE FUNCTION bioprotect.run_impact_pipeline(
    _project_id   integer,
    _activity_ids integer[],
    _profile_name text,
    _description  text DEFAULT ''::text,
    _user         text DEFAULT 'system'::text
)
RETURNS integer
LANGUAGE plpgsql AS $$
DECLARE
    _aid            INTEGER;
    _pressure_count INTEGER;
    _cost_profile   INTEGER;
BEGIN
    FOREACH _aid IN ARRAY _activity_ids LOOP
        SELECT bioprotect.create_pressures_from_activity(_aid)
          INTO _pressure_count;
        RAISE NOTICE 'Activity %: created % pressures', _aid, _pressure_count;
    END LOOP;

    SELECT bioprotect.run_cumulative_impact(
        _project_id, _activity_ids, _profile_name, _description, _user
    ) INTO _cost_profile;

    RETURN _cost_profile;
END;
$$;
