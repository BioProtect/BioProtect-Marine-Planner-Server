-- Migration 0005: Repair activities imported with wrong projection
-- Created: 2026-05-25
--
-- Background:
--   import_shapefile previously hardcoded `-s_srs EPSG:4326`, which made
--   ogr2ogr ignore the .prj file and import non-WGS84 shapefiles with their
--   native (e.g. metre) coordinates mis-tagged as EPSG:4326. The import
--   code is fixed; this migration repairs any rows still affected.
--
-- Strategy:
--   - Each known case is a (activity match, source_srid) pair.
--   - For each matching row whose stored extent is outside the WGS84 valid
--     range, re-tag the geometry's SRID, ST_Transform it to 4326, refresh
--     the metadata extent, and re-derive its pressures.
--   - Idempotent: rows already in WGS84 range are skipped.
--
-- Updates:
--   Tables    : bioprotect.metadata_activities (extent column)
--               bioprotect.activity_* (per-activity geometry tables)
--   Functions : bioprotect.create_pressures_from_activity (invoked)
-- ============================================================

DO $$
DECLARE
    rec        RECORD;
    gtype      TEXT;
    gtype_short TEXT;
    schema_name CONSTANT TEXT := 'bioprotect';
BEGIN
    -- ------------------------------------------------------------
    -- Case 1: Finfish aquaculture — source CRS is IRENET95 / Irish
    -- Transverse Mercator (EPSG:2157).
    -- ------------------------------------------------------------
    FOR rec IN
        SELECT id, activity_name, extent
          FROM bioprotect.metadata_activities
         WHERE activity ILIKE 'Finfish aquaculture'
           AND (
               ST_XMin(extent) < -360 OR ST_XMax(extent) >  360
            OR ST_YMin(extent) <  -90 OR ST_YMax(extent) >   90
           )
    LOOP
        -- Detect geometry type so the ALTER COLUMN cast preserves it
        EXECUTE format(
            'SELECT ST_GeometryType(geometry) FROM %I.%I LIMIT 1',
            schema_name, rec.activity_name
        ) INTO gtype;
        gtype_short := replace(gtype, 'ST_', '');

        -- 1. Re-tag SRID (numbers don't move; just labels the existing coords)
        PERFORM UpdateGeometrySRID(
            schema_name, rec.activity_name, 'geometry', 2157
        );

        -- 2. Reproject to WGS84, preserving the original geometry type
        EXECUTE format(
            'ALTER TABLE %I.%I '
            'ALTER COLUMN geometry TYPE geometry(%s, 4326) '
            'USING ST_Transform(geometry, 4326)',
            schema_name, rec.activity_name, gtype_short
        );

        -- 3. Refresh the cached extent in metadata
        EXECUTE format(
            'UPDATE bioprotect.metadata_activities '
            '   SET extent = (SELECT Box2D(ST_Extent(geometry)) FROM %I.%I) '
            ' WHERE id = %s',
            schema_name, rec.activity_name, rec.id
        );

        -- 4. Re-derive pressures from the corrected geometry
        PERFORM bioprotect.create_pressures_from_activity(rec.id);

        RAISE NOTICE
            'Repaired activity id=% (table=%, source=EPSG:2157 -> 4326)',
            rec.id, rec.activity_name;
    END LOOP;
END $$;
