-- Migration 0010: index the cost_profile_values -> project_pus FK
-- Created: 2026-09-26
--
-- Deleting a project cascades to project_pus (millions of rows across the
-- table), and every deleted PU row fires the FK trigger
--
--     DELETE FROM ONLY cost_profile_values WHERE $1 = project_pu_id
--
-- on an unindexed column, i.e. a sequential scan of cost_profile_values PER
-- DELETED HEX. Deleting a single res-8 project meant ~1M scans of a 180k-row
-- table; the delete never finished and held locks that blocked everything
-- queued behind it.
--
-- Postgres does not index the referencing side of a FK automatically. This is
-- the only unindexed cascade path that matters - the other tables referencing
-- projects are either indexed or empty.
--
-- Updates:
--   Indexes : idx_cost_profile_values_project_pu (new)
-- ============================================================

CREATE INDEX IF NOT EXISTS idx_cost_profile_values_project_pu
    ON bioprotect.cost_profile_values (project_pu_id);
