-- Martin function source for features and activities:
--   /layer_tiles/{z}/{x}/{y}?layer=<feature_class_name | activity_name>
-- One source serves every feature and activity table, so Martin needs no
-- restart when one is created. The MVT layer is named after the table,
-- matching the frontend's source-layer.
CREATE OR REPLACE FUNCTION bioprotect.layer_tiles(z integer, x integer, y integer, query_params json)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
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
  EXECUTE format($q$
    SELECT ST_AsMVT(t, %L, 4096, 'geom') FROM (
      SELECT ST_AsMVTGeom(ST_Transform(geometry, 3857),
                          ST_TileEnvelope($1, $2, $3), 4096, 64, true) AS geom
      FROM bioprotect.%I
      WHERE geometry && ST_Transform(ST_TileEnvelope($1, $2, $3), 4326)
    ) t WHERE geom IS NOT NULL $q$, tbl, tbl)
  INTO mvt USING z, x, y;
  RETURN mvt;
END;
$function$
