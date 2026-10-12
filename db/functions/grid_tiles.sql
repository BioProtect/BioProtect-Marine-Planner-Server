-- Martin function source for planning grids: /grid_tiles/{z}/{x}/{y}?layer=<feature_class_name>
-- One source serves every grid, so Martin needs no restart when a grid is created.
-- The MVT layer is named after the grid, matching the frontend's source-layer.
CREATE OR REPLACE FUNCTION bioprotect.grid_tiles(z integer, x integer, y integer, query_params json)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
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
$function$
