-- Migration 0007: Backfill prioritizr_runs.created_by
-- Created: 2026-09-02
--
-- created_by has existed on prioritizr_runs since the baseline but was never
-- populated by the insert, so every run predating that fix is NULL-owned and
-- therefore undeletable in the UI. There is no record of who actually started
-- those runs, so the closest defensible owner is the project's owner from
-- user_projects. Runs in projects with no owner row stay NULL and remain
-- deletable only by a global Admin.

UPDATE bioprotect.prioritizr_runs r
SET created_by = owner.user_id
FROM (
  SELECT DISTINCT ON (project_id) project_id, user_id
  FROM bioprotect.user_projects
  WHERE role = 'owner'
  ORDER BY project_id, added_at, user_id
) AS owner
WHERE r.created_by IS NULL
  AND r.project_id = owner.project_id;
