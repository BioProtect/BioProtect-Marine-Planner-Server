-- Migration 0008: Drop obsolete 5-arg overload of run_cumulative_impact
<<<<<<< HEAD
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
=======
-- Created: 2026-09-08
--
-- Background:
--   Migrations 0003/0004 created run_cumulative_impact with 5 args
--   (_project_id, _activity_ids, _profile_name, _description, _user).
--   db/functions/run_cumulative_impact.sql later added a 6th argument
--   (_floor NUMERIC DEFAULT 0.001) for the cost-floor rescaling. Because
--   CREATE OR REPLACE only replaces an identical signature, the 6-arg
--   version was created as a *new* overload alongside the old 5-arg one.
--
--   Any 5-arg call is then ambiguous -- the 6th arg has a DEFAULT, so both
--   candidates match -- and Postgres raises:
--     function bioprotect.run_cumulative_impact(integer, integer[],
--     unknown, unknown, unknown) is not unique
--   This hit cost-profile generation via activity_handler and any call to
--   run_impact_pipeline, which invokes run_cumulative_impact with 5 args.
--
-- Fix: drop the old 5-arg signature; keep only the 6-arg one.
-- Same class of bug as migration 0006.

DROP FUNCTION IF EXISTS bioprotect.run_cumulative_impact(integer, integer[], text, text, text);
>>>>>>> 87f0255296932f67b28e1acde1ac172589b28c6a
