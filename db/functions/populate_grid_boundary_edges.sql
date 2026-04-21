CREATE OR REPLACE FUNCTION bioprotect.populate_grid_boundary_edges(
  p_planning_unit_id INT
)
RETURNS BIGINT
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
