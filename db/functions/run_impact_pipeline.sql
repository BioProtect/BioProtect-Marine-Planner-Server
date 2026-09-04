CREATE OR REPLACE FUNCTION bioprotect.run_impact_pipeline(
    _project_id    INTEGER,
    _activity_ids  INTEGER[],
    _profile_name  TEXT,
    _description   TEXT DEFAULT '',
    _user          TEXT DEFAULT 'system'
)
RETURNS INTEGER  -- cost_profile_id
LANGUAGE plpgsql
AS $$
DECLARE
    _aid            INTEGER;
    _pressure_count INTEGER;
    _cost_profile   INTEGER;
BEGIN
    -- Create pressures for each activity
    FOREACH _aid IN ARRAY _activity_ids LOOP
        SELECT bioprotect.create_pressures_from_activity(_aid)
          INTO _pressure_count;
        RAISE NOTICE 'Activity %: created % pressures', _aid, _pressure_count;
    END LOOP;

    -- Run cumulative impact
    SELECT bioprotect.run_cumulative_impact(
        _project_id, _activity_ids, _profile_name, _description, _user
    ) INTO _cost_profile;

    RETURN _cost_profile;
END;
$$;
