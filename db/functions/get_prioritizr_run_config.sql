DROP FUNCTION IF EXISTS bioprotect.get_prioritizr_run_config(bigint);
CREATE OR REPLACE FUNCTION bioprotect.get_prioritizr_run_config(p_run_id bigint)
RETURNS TABLE (
  run_id bigint,
  project_id int,
  input_table text,
  feature_cols text[],
  target_prop double precision,
  mode text,
  boundary_penalty double precision,
  linear_cost_penalty double precision,
  gap double precision,
  time_limit_sec int,
  feature_targets_json text
)
LANGUAGE sql
STABLE
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
  ) AS feature_targets_json
FROM bioprotect.prioritizr_runs r
WHERE r.id = p_run_id;
$$;
