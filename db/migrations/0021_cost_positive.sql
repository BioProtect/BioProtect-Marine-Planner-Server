-- Migration 0021: no planning unit ever costs 0
-- Created: 2026-11-12
--
-- Every cost writer floors at 0.001 (run_cumulative_impact's _floor, the
-- raster cost handler's floor), but cumulative-impact profiles made before
-- the floor existed (Apr-May 2026) still hold zeros. In min-set a free cell
-- can be picked or dropped at no cost and the clustering discount does
-- nothing to it. Raise the old zeros to the same floor, then refuse new ones.

UPDATE bioprotect.cost_profile_values SET cost = 0.001 WHERE cost <= 0;

ALTER TABLE bioprotect.cost_profile_values
    ADD CONSTRAINT cost_profile_values_cost_positive CHECK (cost > 0);
