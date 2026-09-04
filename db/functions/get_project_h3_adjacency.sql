CREATE OR REPLACE FUNCTION bioprotect.get_project_h3_adjacency(
  p_project_id INT
)
RETURNS TABLE (
  pu_id TEXT,
  nbr_id TEXT,
  boundary DOUBLE PRECISION
)
LANGUAGE sql
STABLE
AS
$$
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
