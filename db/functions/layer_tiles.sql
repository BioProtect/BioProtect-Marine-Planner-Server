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
  -- Simplify to a quarter pixel, then clip to the tile (+ MVT buffer), THEN
  -- transform. Unsimplified, every tile transformed and clipped whole source
  -- polygons: the CSO Sublittoral Sediments layer (58k polygons, one with
  -- 496k vertices and 42k holes) took 104 s for one z4 tile -> 504 from nginx.
  -- Now the worst tile is ~2.4 s (z8). Simplify before clip: clipping that
  -- polygon at full detail alone costs ~4 s.
  -- ponytail: at high zoom the simplify barely reduces it, so the big polygon
  -- still costs ~2 s/tile there; feature_pieces could serve high zooms if needed.
  EXECUTE format($q$
    SELECT ST_AsMVT(t, %L, 4096, 'geom') FROM (
      SELECT ST_AsMVTGeom(
               ST_Transform(ST_ClipByBox2D(ST_Simplify(geometry, $4), $5), 3857),
               ST_TileEnvelope($1, $2, $3), 4096, 64, true) AS geom
      FROM bioprotect.%I
      WHERE geometry && $5
    ) t WHERE geom IS NOT NULL $q$, tbl, tbl)
  INTO mvt USING z, x, y,
    360.0 / (256 * 2 ^ z) / 4,  -- quarter of a 256px-tile pixel, in degrees
    ST_Transform(ST_TileEnvelope(z, x, y, margin => 64.0 / 4096), 4326);
  RETURN mvt;
END;
$function$
