-- Migration 0006: Drop obsolete 3-arg overload of set_active_profile_pu_statuses
-- Created: 2026-06-23
--
-- Background:
--   Migration 0003 added a 4-arg version (with p_status3_h3) via
--   CREATE OR REPLACE FUNCTION. Because the signature differs from the
--   original 3-arg version, both ended up coexisting as overloads.
--   Callers passing 3 args (e.g. planning_unit_handler.update_planning_units)
--   hit "function ... is not unique" because Postgres can satisfy either
--   overload via DEFAULTs, especially when the first array literal is empty
--   and typed as `unknown`.
--
-- Fix: drop the old 3-arg signature; keep only the 4-arg one.

DROP FUNCTION IF EXISTS bioprotect.set_active_profile_pu_statuses(integer, text[], text[]);
