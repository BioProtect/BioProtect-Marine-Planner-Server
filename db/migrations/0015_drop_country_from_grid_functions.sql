-- Migration 0015: drop get_pu_grids() and get_planning_units_metadata()
-- Created: 2026-10-29
--
-- `country` (and get_pu_grids' `country_id`) left their return types; it was
-- always NULL (gaul_2015_simplified_1km is empty). CREATE OR REPLACE can't
-- change a return type, so drop here and let db/functions/ recreate them:
-- `migrate.py deploy` runs migrations first, then functions. If you run
-- `migrate` on its own, run those two function files straight after.

DROP FUNCTION IF EXISTS bioprotect.get_pu_grids();
DROP FUNCTION IF EXISTS bioprotect.get_planning_units_metadata(bigint);
