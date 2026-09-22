-- Diagnose: a feature was added + preprocessed, but does not appear in a Prioritizr solution.
--
-- Usage (psql):
--   \set project_id 56
--   \set feature_id 123
--   \set run_id     789
--   \i server/db/diagnostics/diagnose_missing_feature.sql
--
-- Each step prints one row. The FIRST step that reports a problem is the break.

\echo '--- 1. Is the feature linked to the project? (empty = never reached project_features)'
SELECT pf.feature_unique_id, pf.target_type, pf.target_value, pf.spf, pf.created_at
FROM bioprotect.project_features pf
WHERE pf.project_id = :project_id
  AND pf.feature_unique_id = :feature_id;

\echo '--- 2. Did preprocessing write any amounts? (rows=0 => nothing intersected)'
SELECT count(*) AS rows,
       sum(amount) AS total_km2,
       min(amount) AS min_amount,
       max(amount) AS max_amount
FROM bioprotect.pu_feature_amounts
WHERE project_id = :project_id
  AND feature_unique_id = :feature_id;

\echo '--- 3. Do those amounts land on hexes that are actually in this project grid?'
\echo '       matched_pus = 0 with rows > 0 in step 2 means the WRONG planning grid was used.'
SELECT count(*) FILTER (WHERE pp.id IS NOT NULL) AS matched_pus,
       count(*)                                   AS amount_rows
FROM bioprotect.pu_feature_amounts pfa
LEFT JOIN bioprotect.project_pus pp
       ON pp.project_id = pfa.project_id
      AND pp.h3_index   = pfa.h3_index
WHERE pfa.project_id = :project_id
  AND pfa.feature_unique_id = :feature_id;

\echo '--- 4. Sanity: which grid do this project''s PUs come from vs. what preprocessing used?'
SELECT p.id AS project_id,
       hc.project_area,
       hc.resolution,
       count(*) AS project_pu_count
FROM bioprotect.projects p
JOIN bioprotect.project_pus pp ON pp.project_id = p.id
JOIN bioprotect.h3_cells   hc ON hc.h3_index = pp.h3_index
WHERE p.id = :project_id
GROUP BY p.id, hc.project_area, hc.resolution;

\echo '--- 5. Does the feature geometry overlap the project extent at all?'
SELECT mif.unique_id,
       mif.alias,
       mif.feature_class_name,
       ST_Intersects(
         ST_SetSRID(mif.extent::geometry, 4326),
         (SELECT ST_Extent(hc.geometry)::geometry
            FROM bioprotect.project_pus pp
            JOIN bioprotect.h3_cells hc ON hc.h3_index = pp.h3_index
           WHERE pp.project_id = :project_id)
       ) AS overlaps_project_extent
FROM bioprotect.metadata_interest_features mif
WHERE mif.unique_id = :feature_id;

\echo '--- 6. Did the run build a column for it? (feature_cols must contain f_<feature_id>)'
SELECT r.id AS run_id,
       r.input_table,
       ('f_' || :feature_id) = ANY(r.feature_cols) AS column_was_built,
       array_length(r.feature_cols, 1) AS n_feature_cols
FROM bioprotect.prioritizr_runs r
WHERE r.id = :run_id;

\echo '--- 7. Is the column non-zero in the prepared input table?'
\echo '       If total = 0 the R script silently dropped this feature (run_prioritzr_v2.R zero-coverage filter).'
SELECT format(
  'SELECT count(*) AS pus, count(*) FILTER (WHERE f_%s > 0) AS pus_with_feature, sum(f_%s) AS total FROM %s',
  :feature_id, :feature_id,
  (SELECT input_table FROM bioprotect.prioritizr_runs WHERE id = :run_id)
) AS run_this_next \gset
:run_this_next

\echo '--- 8. What did the R log say it used? (look for "Using features:")'
SELECT message
FROM bioprotect.prioritizr_run_logs
WHERE run_id = :run_id
  AND (message LIKE '%Using features%' OR message LIKE '%Features:%')
ORDER BY id;
