-- Drops a run's per-run input table when the run row goes away.
--
-- prepare_prioritizr_input() creates one bioprotect.prioritizr_input_run_<id>
-- table per run and records the name on prioritizr_runs.input_table. Without
-- this trigger those tables outlive their runs: the run row is deleted (by the
-- API, by a project cascade, or by hand in psql) and the input table is left
-- orphaned in the schema forever.
--
-- The regex guard means the trigger only ever drops tables matching the name
-- that function generates — never anything else that lands in the column.

CREATE OR REPLACE FUNCTION bioprotect.drop_prioritizr_input_table()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
  IF OLD.input_table ~ '^bioprotect\.prioritizr_input_run_\d+$' THEN
    EXECUTE 'DROP TABLE IF EXISTS ' || OLD.input_table;
  END IF;
  RETURN OLD;
END;
$$;

DROP TRIGGER IF EXISTS trg_drop_prioritizr_input_table
  ON bioprotect.prioritizr_runs;

CREATE TRIGGER trg_drop_prioritizr_input_table
AFTER DELETE ON bioprotect.prioritizr_runs
FOR EACH ROW
EXECUTE FUNCTION bioprotect.drop_prioritizr_input_table();
