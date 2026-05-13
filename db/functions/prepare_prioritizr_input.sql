-- Given run_id:
--  - finds project_id
--  - finds active_cost_profile_id
--  - gets the project’s selected features (project_features)
--  - builds an UNLOGGED wide table with:
--     - pu_id (h3_index)
--     - geometry (from h3_cells)
--     - cost (from cost_profile_values)
--     - area_km2 (computed)
--     - one numeric column per feature: f_<feature_id>
--     - stores input_table + feature_cols back onto the run row


CREATE OR REPLACE FUNCTION bioprotect.prepare_prioritizr_input(p_run_id bigint)
RETURNS void
LANGUAGE plpgsql
AS $$
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
$$;
