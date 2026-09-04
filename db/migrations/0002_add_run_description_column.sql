-- Migration 0002: Add description column to prioritizr_runs
-- Created: 2026-04-10
--
-- Adds a free-form description column so users can record what each run is
-- for. The label column (already present) holds the short name shown in the
-- runs list.

ALTER TABLE bioprotect.prioritizr_runs
  ADD COLUMN IF NOT EXISTS description TEXT;
