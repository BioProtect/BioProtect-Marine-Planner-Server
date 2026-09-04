-- Migration 0001: Baseline
-- Created: 2026-04-09
--
-- This is a no-op migration that marks the existing database state as the baseline.
-- All tables, functions, views, and data that exist as of this date are considered
-- the starting point. Future migrations build on top of this.
--
-- The actual schema was created via:
--   - sql_scripts/schema.sql (original dump)
--   - migrations/001_impact_system_redesign.sql (applied manually)
--   - Various sql_scripts/*.sql applied ad hoc
--
-- All functions are now managed in db/functions/ (idempotent, re-deployable).

-- Nothing to do — this migration just establishes the tracking baseline.
SELECT 1;
