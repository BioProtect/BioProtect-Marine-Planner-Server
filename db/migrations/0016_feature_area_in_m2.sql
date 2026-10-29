-- Migration 0016: recompute metadata_interest_features._area in m²
-- Created: 2026-10-29
--
-- finish_feature_import stored ST_Area(ST_Transform(geom, 4326)) - a no-op
-- transform on 4326 data, so square degrees - while the frontend reads _area
-- as m² and Engage stored m² (EPSG:3410). Recompute every polygon feature the
-- same way the import now does: ST_Area(ST_Union(geometry)::geography).
-- Point features keep their _area: it holds the summed amount, not an area.
-- Features whose table is gone, or whose geometry won't union, are left
-- alone (a NOTICE names them) rather than failing the whole migration.

DO $$
DECLARE
  f record;
  gtype text;
  area double precision;
BEGIN
  FOR f IN SELECT unique_id, feature_class_name
             FROM bioprotect.metadata_interest_features LOOP
    CONTINUE WHEN to_regclass(format('bioprotect.%I', f.feature_class_name)) IS NULL;
    EXECUTE format('SELECT GeometryType(geometry) FROM bioprotect.%I LIMIT 1',
                   f.feature_class_name) INTO gtype;
    CONTINUE WHEN gtype IS NULL OR gtype NOT IN ('POLYGON', 'MULTIPOLYGON');
    BEGIN
      EXECUTE format('SELECT ST_Area(ST_Union(geometry)::geography) FROM bioprotect.%I',
                     f.feature_class_name) INTO area;
    EXCEPTION WHEN OTHERS THEN
      RAISE NOTICE 'skipped %: %', f.feature_class_name, SQLERRM;
      CONTINUE;
    END;
    UPDATE bioprotect.metadata_interest_features
       SET _area = area
     WHERE unique_id = f.unique_id;
  END LOOP;
END $$;
