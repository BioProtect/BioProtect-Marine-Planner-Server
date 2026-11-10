-- Migration 0017: drop SQL functions nothing calls
-- Created: 2026-11-10
--
-- No caller in server/, frontend/, the R script, Engage, SysConPriotitizr or
-- the Martin config, and no function/view/trigger in the DB references them.
-- get_project_pus selected cost/status columns project_pus no longer has;
-- run_impact_pipeline called the 5-arg run_cumulative_impact that 0008 dropped;
-- the rest are Marxan-server leftovers. Their files in db/functions/ are gone
-- too, so deploy won't recreate them.

DROP FUNCTION IF EXISTS bioprotect.deletedissolvedwdpafeatureclasses();
DROP FUNCTION IF EXISTS bioprotect.deleteorphanedfeatures();
DROP FUNCTION IF EXISTS bioprotect.deletescratchfeatureclasses();
DROP FUNCTION IF EXISTS bioprotect.get_project_pus(integer);
DROP FUNCTION IF EXISTS bioprotect.get_pu_areas_for_interest_feature(text, text);
DROP FUNCTION IF EXISTS bioprotect.run_impact_pipeline(integer, integer[], text, text, text);
DROP FUNCTION IF EXISTS bioprotect.updatefeaturesbatch();
DROP FUNCTION IF EXISTS bioprotect.updateplanningunitsbatch();
