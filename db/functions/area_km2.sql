-- Area helpers for the per-project CRS (migration 0012).
--
-- project_area_srid(project_id): the SRID to measure areas in, or NULL to
-- measure on the geography ellipsoid. NULL for geographic CRSs (4326, 4167,
-- ...) whose units are degrees, and for projected CRSs not in metres
-- (US-feet State Plane etc.), so area_km2 always returns km². Resolve it once
-- per query, not per row.
--
-- area_km2(geom, srid): area in km². Plain SQL and IMMUTABLE so it inlines.
--
-- project_area_srid is plpgsql, not sql: `migrate.py deploy` creates functions
-- before running migrations, and a sql body is checked against projects.crs_epsg
-- at CREATE time, before the column exists.

CREATE OR REPLACE FUNCTION bioprotect.project_area_srid(p_project_id integer)
 RETURNS integer
 LANGUAGE plpgsql
 STABLE
AS $function$
BEGIN
    RETURN (
        SELECT CASE
                 WHEN srs.proj4text LIKE '%+units=m %' OR srs.proj4text LIKE '%+units=m'
                   THEN p.crs_epsg
               END
          FROM bioprotect.projects p
          LEFT JOIN public.spatial_ref_sys srs ON srs.srid = p.crs_epsg
         WHERE p.id = p_project_id
    );
END;
$function$;

CREATE OR REPLACE FUNCTION bioprotect.area_km2(p_geom geometry, p_srid integer)
 RETURNS double precision
 LANGUAGE sql
 IMMUTABLE
AS $function$
    SELECT CASE
             WHEN p_srid IS NULL THEN ST_Area(p_geom::geography)
             ELSE ST_Area(ST_Transform(p_geom, p_srid))
           END / 1000000.0
$function$;
