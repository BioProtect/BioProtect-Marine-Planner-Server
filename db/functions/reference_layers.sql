-- User reference layers (see migrations/0020_reference_layers.sql).
-- Upload: ogr2ogr loads a scratch table (plus a `_ref_id` row number), then
-- reference_layer_batches splits it into vertex-weighted id ranges and
-- reference_layer_add_shapes copies each range in, so no single query hits
-- the 120 s pool cap on big files.

-- Source-id ranges of ~p_batch_points vertices each. A row starts in the batch
-- where its vertices start, so a giant shape shares its batch only with the
-- small rows before it (same scheme as feature_piece_batches).
CREATE OR REPLACE FUNCTION bioprotect.reference_layer_batches(
    p_scratch text, p_batch_points integer)
 RETURNS TABLE(from_id integer, to_id integer)
 LANGUAGE plpgsql
AS $function$
BEGIN
    RETURN QUERY EXECUTE format($f$
        WITH v AS (
            SELECT _ref_id, floor((sum(ST_NPoints(geometry)) OVER (ORDER BY _ref_id)
                              - ST_NPoints(geometry)) / $1) AS batch
              FROM bioprotect.%I
        )
        SELECT min(_ref_id), max(_ref_id) FROM v GROUP BY batch ORDER BY batch
    $f$, p_scratch) USING p_batch_points;
END;
$function$;

-- Copies scratch rows with id in [from, to] into reference_layer_shapes.
-- Attributes are every shapefile column (not geometry, ogc_fid or _ref_id).
CREATE OR REPLACE FUNCTION bioprotect.reference_layer_add_shapes(
    p_layer_id integer, p_scratch text, p_geom_type text,
    p_from_id integer, p_to_id integer)
 RETURNS void
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_dim integer := CASE p_geom_type WHEN 'polygon' THEN 3 WHEN 'line' THEN 2 ELSE 1 END;
BEGIN
    -- Repair, keep only the layer's geometry type, one row per part, size
    -- (km² / km) worked out once per part.
    -- Every set-returning call is in FROM (LATERAL), never in a select list:
    -- there the other select-list expressions re-run for every row it returns.
    -- to_jsonb(s) (which serialises the geometry as GeoJSON before '- geometry'
    -- drops it) next to ST_Dump, and ST_Area next to ST_Subdivide, each took a
    -- 7,531-part, 496k-vertex CSO shape from seconds to about an hour.
    EXECUTE format($f$
        CREATE TEMP TABLE ref_src ON COMMIT DROP AS
        WITH src AS MATERIALIZED (
            SELECT to_jsonb(s) - 'geometry' - '_ref_id' - 'ogc_fid' AS attributes,
                   s.geometry
              FROM bioprotect.%I s
             WHERE s._ref_id BETWEEN $2 AND $3 AND s.geometry IS NOT NULL
        )
        SELECT src.attributes, d.geom,
               CASE $1 WHEN 3 THEN ST_Area(d.geom::geography) / 1e6
                       WHEN 2 THEN ST_Length(d.geom::geography) / 1e3 END AS size_km
          FROM src, LATERAL ST_Dump(ST_CollectionExtract(ST_MakeValid(src.geometry), $1)) d
    $f$, p_scratch) USING v_dim, p_from_id, p_to_id;

    IF p_geom_type = 'polygon' THEN
        INSERT INTO bioprotect.reference_layer_shapes (layer_id, part, attributes, size_km, geom)
        SELECT p_layer_id, 'fill', r.attributes, r.size_km, sd.geom
          FROM ref_src r, LATERAL ST_Subdivide(r.geom, 256) sd(geom)
        UNION ALL
        SELECT p_layer_id, 'outline', NULL, r.size_km, sd.geom
          FROM ref_src r, LATERAL ST_Subdivide(ST_Boundary(r.geom), 256) sd(geom);
    ELSIF p_geom_type = 'line' THEN
        INSERT INTO bioprotect.reference_layer_shapes (layer_id, part, attributes, size_km, geom)
        SELECT p_layer_id, 'line', r.attributes, r.size_km, sd.geom
          FROM ref_src r, LATERAL ST_Subdivide(r.geom, 256) sd(geom);
    ELSE
        INSERT INTO bioprotect.reference_layer_shapes (layer_id, part, attributes, geom)
        SELECT p_layer_id, 'point', attributes, geom FROM ref_src;
    END IF;
    DROP TABLE ref_src;
END;
$function$;

-- Martin function source: /reference_tiles/{z}/{x}/{y}?layer=<id>
-- MVT layers ref_fill / ref_outline / ref_line / ref_point. Attributes go in
-- as one JSON string (attrs), parsed by the frontend on click: works on any
-- PostGIS, and a shapefile's own column names can't clash with MVT keys.
-- Polygons smaller than a pixel and lines shorter than one are skipped.
CREATE OR REPLACE FUNCTION bioprotect.reference_tiles(
    z integer, x integer, y integer, query_params json)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
DECLARE
  v_layer   integer;
  env       geometry := ST_TileEnvelope(z, x, y);
  env4326   geometry := ST_Transform(ST_TileEnvelope(z, x, y), 4326);
  px_deg    double precision := 360.0 / (256 * 2 ^ z);
  px_km     double precision := 40075.0 / (256 * 2 ^ z);
  out_mvt   bytea := '';
  part_mvt  bytea;
  p         text;
BEGIN
  BEGIN
    v_layer := (query_params->>'layer')::integer;
  EXCEPTION WHEN others THEN
    RETURN NULL;
  END;
  IF v_layer IS NULL THEN
    RETURN NULL;
  END IF;

  FOREACH p IN ARRAY ARRAY['fill', 'outline', 'line', 'point'] LOOP
    SELECT ST_AsMVT(t, 'ref_' || p, 4096, 'geom') INTO part_mvt FROM (
      SELECT s.attributes::text AS attrs,
             ST_AsMVTGeom(
               ST_Transform(CASE WHEN p = 'point' THEN s.geom
                                 ELSE ST_Simplify(s.geom, px_deg / 4) END, 3857),
               env, 4096, 64, true) AS geom
        FROM bioprotect.reference_layer_shapes s
       WHERE s.layer_id = v_layer AND s.part = p AND s.geom && env4326
         AND (p = 'point'
              OR (p IN ('fill', 'outline') AND s.size_km >= px_km ^ 2)
              OR (p = 'line' AND s.size_km >= px_km))
    ) t WHERE geom IS NOT NULL;
    out_mvt := out_mvt || coalesce(part_mvt, '');
  END LOOP;
  RETURN out_mvt;
END;
$function$;
