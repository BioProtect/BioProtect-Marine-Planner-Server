-- Martin function source for the WDPA reference layer: /wdpa_tiles/{z}/{x}/{y}
-- Two MVT layers per tile:
--   wdpa_fill  polygon pieces with the attributes the click info shows
--   wdpa_line  boundaries (subdivided separately, so no seams; see 0013)
-- Areas smaller than a pixel at this zoom are skipped: they'd be invisible,
-- and at world scale they're most of the rows.
CREATE OR REPLACE FUNCTION bioprotect.wdpa_tiles(z integer, x integer, y integer)
 RETURNS bytea
 LANGUAGE plpgsql
 STABLE PARALLEL SAFE
AS $function$
DECLARE
  env       geometry := ST_TileEnvelope(z, x, y);
  env4326   geometry := ST_Transform(ST_TileEnvelope(z, x, y), 4326);
  -- one 256px-tile pixel: degrees (for simplifying) and km² (for culling)
  px_deg    double precision := 360.0 / (256 * 2 ^ z);
  px_km2    double precision := (40075.0 / (256 * 2 ^ z)) ^ 2;
  fill_mvt  bytea;
  line_mvt  bytea;
BEGIN
  SELECT ST_AsMVT(t, 'wdpa_fill', 4096, 'geom') INTO fill_mvt FROM (
    SELECT wdpaid, name, desig_eng, iucn_cat, status, status_yr,
           ST_AsMVTGeom(ST_Transform(ST_Simplify(geom, px_deg / 4), 3857),
                        env, 4096, 64, true) AS geom
    FROM bioprotect.wdpa_areas
    WHERE part = 'fill' AND geom && env4326 AND area_km2 >= px_km2
  ) t WHERE geom IS NOT NULL;

  SELECT ST_AsMVT(t, 'wdpa_line', 4096, 'geom') INTO line_mvt FROM (
    SELECT wdpaid,
           ST_AsMVTGeom(ST_Transform(ST_Simplify(geom, px_deg / 4), 3857),
                        env, 4096, 64, true) AS geom
    FROM bioprotect.wdpa_areas
    WHERE part = 'line' AND geom && env4326 AND area_km2 >= px_km2
  ) t WHERE geom IS NOT NULL;

  RETURN coalesce(fill_mvt, '') || coalesce(line_mvt, '');
END;
$function$;
