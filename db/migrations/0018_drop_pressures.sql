-- Migration 0018: drop the pressures table and create_pressures_from_activity
-- Created: 2026-11-10
--
-- pressures held, per PAD row of an activity, a copy of the activity's merged
-- footprint (3 activities = 65 rows = 125 MB on dev). run_cumulative_impact now
-- joins PAD on the activity title and tests cells against the activity's own
-- polygons, so every value here is derivable. The ST_Union that built it was
-- also what large raster activities could not get through.
-- No view, FK or other function referenced either. The function's file in
-- db/functions/ is gone, so deploy won't recreate it.

DROP FUNCTION IF EXISTS bioprotect.create_pressures_from_activity(integer);
DROP TABLE IF EXISTS bioprotect.pressures;
