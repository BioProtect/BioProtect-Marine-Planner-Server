CREATE OR REPLACE FUNCTION bioprotect.compare_runs(p_run_ids bigint[])
 RETURNS TABLE(project_id integer, h3_index text, avg_pct double precision, shares jsonb)
 LANGUAGE sql
 STABLE
 -- CTE row estimates are wildly high, which turns JIT on for a ~0.4 s
 -- compile on a query that runs in less
 SET jit = off
AS $function$
-- Compare Prioritizr runs across projects, cell by cell.
--
-- Each project's share of a cell is the % of that project's chosen runs that
-- selected it. A cell is covered by every project that has the same cell or
-- an ancestor of it (a coarser cell containing it), so a selected res-7 cell
-- counts for each res-9 cell inside it. avg_pct is the mean of the covering
-- projects' shares; shares is {project_id: pct} for the hover breakdown.
-- Finer projects do not cover coarser cells: the map draws coarse grids
-- below fine ones, so the fine cells carry the comparison where they exist.
--
-- One row per (project, own cell) with at least one share above zero.
WITH runs AS (
    SELECT r.id, r.project_id
    FROM   bioprotect.prioritizr_runs r
    WHERE  r.id = ANY(p_run_ids)
),
n_runs AS (
    SELECT runs.project_id, count(*) AS n FROM runs GROUP BY runs.project_id
),
picked AS (
    SELECT runs.project_id, rr.h3_index, count(*) AS c
    FROM   runs
    JOIN   bioprotect.prioritizr_run_results rr
             ON rr.run_id = runs.id AND rr.solution = 1
    GROUP  BY runs.project_id, rr.h3_index
),
share AS (
    -- every cell of every compared project, zeros included: a coarse cell
    -- nobody picked still pulls the average down for the fine cells inside it
    SELECT pp.project_id, pp.h3_index,
           h3_get_resolution(pp.h3_index::h3index) AS res,
           100.0 * COALESCE(p.c, 0) / n.n AS pct
    FROM   bioprotect.project_pus pp
    JOIN   n_runs n ON n.project_id = pp.project_id
    LEFT JOIN picked p ON p.project_id = pp.project_id AND p.h3_index = pp.h3_index
),
res_set AS (
    SELECT DISTINCT share.project_id, share.res FROM share
),
cand AS (
    -- only cells some selected cell covers can come out non-zero: the
    -- selected cells' own copies and their children in finer projects.
    -- Without this every zero cell of a regional grid goes through `cover`.
    -- ponytail: 7^(res gap) children per selected cell; fine for 7 vs 9,
    -- revisit if someone compares res 5 against res 10.
    SELECT DISTINCT s.project_id, s.h3_index, s.res
    FROM   share pc
    JOIN   res_set q
             ON q.res >= pc.res
            AND (q.project_id <> pc.project_id OR q.res = pc.res)
    CROSS JOIN LATERAL (
        SELECT pc.h3_index AS h WHERE q.res = pc.res
        UNION ALL
        SELECT ch::text FROM h3_cell_to_children(pc.h3_index::h3index, q.res) ch
        WHERE q.res > pc.res
    ) c
    JOIN   share s ON s.project_id = q.project_id AND s.h3_index = c.h
    WHERE  pc.pct > 0
),
cover AS (
    -- for each cell, the covering cell (itself or an ancestor) in each project
    SELECT s.project_id, s.h3_index, q.project_id AS q_project, sq.pct
    FROM   cand s
    JOIN   res_set q
             ON q.res <= s.res
            -- a project covers its own cells only at their own resolution
            AND (q.project_id <> s.project_id OR q.res = s.res)
    JOIN   share sq
             ON sq.project_id = q.project_id
            AND sq.h3_index = CASE WHEN q.res = s.res THEN s.h3_index
                                   ELSE h3_cell_to_parent(s.h3_index::h3index, q.res)::text
                              END
),
per_project AS (
    -- a project with cells at two resolutions could cover a cell twice
    SELECT c.project_id, c.h3_index, c.q_project, avg(c.pct) AS pct
    FROM   cover c
    GROUP  BY c.project_id, c.h3_index, c.q_project
)
SELECT pp.project_id, pp.h3_index,
       avg(pp.pct)::double precision AS avg_pct,
       jsonb_object_agg(pp.q_project, round(pp.pct, 1)) AS shares
FROM   per_project pp
GROUP  BY pp.project_id, pp.h3_index
HAVING max(pp.pct) > 0;
$function$
