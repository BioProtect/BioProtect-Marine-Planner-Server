-- Builds bioprotect.wdpa_areas from bioprotect.wdpa_staging (loaded by
-- db/load_wdpa.sh) and swaps it in. Tiles keep serving the old data until
-- the swap at the end, which only holds a lock for a moment.
--
-- wdpa_staging: columns wdpaid, name, desig_eng, iucn_cat, status,
-- status_yr and geom (polygons, EPSG:4326), already filtered to marine and
-- coastal areas. It is dropped afterwards.
CREATE OR REPLACE FUNCTION bioprotect.wdpa_publish(p_release text)
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
    v_areas bigint;
BEGIN
    DROP TABLE IF EXISTS bioprotect.wdpa_areas_new;
    CREATE TABLE bioprotect.wdpa_areas_new (LIKE bioprotect.wdpa_areas INCLUDING DEFAULTS INCLUDING CONSTRAINTS);

    WITH area AS (
        SELECT s.wdpaid::bigint AS wdpaid, s.name, s.desig_eng, s.iucn_cat,
               s.status, s.status_yr::integer AS status_yr,
               ST_CollectionExtract(ST_MakeValid(s.geom), 3) AS geom
        FROM bioprotect.wdpa_staging s
    ), poly AS (
        SELECT a.*, ST_Area(a.geom::geography) / 1e6 AS area_km2, d.geom AS poly
        FROM area a, ST_Dump(a.geom) d
        WHERE NOT ST_IsEmpty(a.geom)
    )
    INSERT INTO bioprotect.wdpa_areas_new
        (wdpaid, name, desig_eng, iucn_cat, status, status_yr, area_km2, part, geom)
    SELECT wdpaid, name, desig_eng, iucn_cat, status, status_yr, area_km2, 'fill', p.geom
    FROM poly, ST_Subdivide(poly, 256) s(geom), ST_Dump(s.geom) p
    WHERE GeometryType(p.geom) = 'POLYGON'
    UNION ALL
    SELECT wdpaid, NULL, NULL, NULL, NULL, NULL, area_km2, 'line', p.geom
    FROM poly, ST_Subdivide(ST_Boundary(poly), 256) s(geom), ST_Dump(s.geom) p
    WHERE GeometryType(p.geom) = 'LINESTRING';

    CREATE INDEX wdpa_areas_new_geom_gist ON bioprotect.wdpa_areas_new USING gist (geom);
    ANALYZE bioprotect.wdpa_areas_new;

    -- the swap
    DROP TABLE bioprotect.wdpa_areas;
    ALTER TABLE bioprotect.wdpa_areas_new RENAME TO wdpa_areas;
    ALTER INDEX bioprotect.wdpa_areas_new_geom_gist RENAME TO wdpa_areas_geom_gist;
    DELETE FROM bioprotect.wdpa_release;
    INSERT INTO bioprotect.wdpa_release (release) VALUES (p_release);
    DROP TABLE bioprotect.wdpa_staging;

    SELECT count(DISTINCT wdpaid) INTO v_areas FROM bioprotect.wdpa_areas;
    RETURN v_areas;
END;
$function$;
