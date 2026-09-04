CREATE OR REPLACE FUNCTION bioprotect.get_project_boundary_edges(
  p_project_id INT
)
RETURNS TABLE (
  pu_id    TEXT,
  nbr_id   TEXT,
  boundary DOUBLE PRECISION
)
LANGUAGE sql
STABLE
AS $$
  SELECT gbe.h3_a AS pu_id,
         gbe.h3_b AS nbr_id,
         gbe.boundary
  FROM bioprotect.grid_boundary_edges gbe
  JOIN bioprotect.projects pr ON pr.planning_unit_id = gbe.planning_unit_id
  WHERE pr.id = p_project_id;
$$;
