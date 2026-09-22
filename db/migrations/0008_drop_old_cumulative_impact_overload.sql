-- Migration 0008: Drop obsolete 5-arg overload of run_cumulative_impact
-- Created: 2026-09-08 (file reconstructed 2026-09-26 - see note below)
--
-- Background:
--   Migrations 0003 and 0004 create run_cumulative_impact with five
--   arguments (_project_id, _activity_ids, _profile_name, _description,
--   _user). db/functions/run_cumulative_impact.sql later added a sixth,
--   _floor NUMERIC DEFAULT 0.001. Because the signature differs, the two
--   coexist as overloads, and activity_handler.py calls the function with
--   exactly five arguments - which Postgres can satisfy from either, so
--   the call fails with "function ... is not unique".
--
--   Same failure mode as migration 0006, different function.
--
-- Fix: drop the old 5-arg signature; keep only the 6-arg one that
-- db/functions/ deploys. Migrations run before functions are deployed, so
-- the drop happens first and the function directory then installs the
-- single surviving version.
--
-- NOTE ON THIS FILE:
--   Version 0008 was applied to the live database on 2026-09-08, but the
--   file was never committed to this working copy - leaving a hole in the
--   sequence and, worse, a fresh install that would skip this drop and end
--   up with both overloads. Reconstructed from the recorded migration name
--   and the state of the live database (which has only the 6-arg version).
--   It is a no-op where 0008 is already recorded as applied.

DROP FUNCTION IF EXISTS bioprotect.run_cumulative_impact(
    integer, integer[], text, text, text
);
