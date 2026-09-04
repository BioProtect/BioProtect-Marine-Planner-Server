CREATE OR REPLACE FUNCTION bioprotect.create_pressures_from_activity(_activity_id integer)
 RETURNS integer
 LANGUAGE plpgsql
AS $function$
DECLARE
    _activity_title TEXT;
    _activity_table TEXT;
    _activity_geom  geometry;
    _count          INTEGER;
BEGIN
    -- Get activity metadata
    SELECT activity, activity_name
      INTO _activity_title, _activity_table
      FROM bioprotect.metadata_activities
     WHERE id = _activity_id;

    IF _activity_title IS NULL THEN
        RAISE EXCEPTION 'Activity % not found', _activity_id;
    END IF;

    IF _activity_table IS NULL THEN
        RAISE EXCEPTION 'Activity % has no geometry table', _activity_id;
    END IF;

    -- Union all geometries from the activity table into one
    EXECUTE format(
        'SELECT ST_Union(geometry) FROM bioprotect.%I',
        _activity_table
    ) INTO _activity_geom;

    IF _activity_geom IS NULL THEN
        RAISE EXCEPTION 'Activity table % has no geometry data', _activity_table;
    END IF;

    -- Delete any existing pressures for this activity (re-runnable)
    DELETE FROM bioprotect.pressures WHERE activity_id = _activity_id;

    -- Insert one pressure per PAD entry for this activity
    INSERT INTO bioprotect.pressures
        (activity_id, pressuretitle, rppscore, geometry)
    SELECT _activity_id,
           pad.pressuretitle,
           pad.rppscore,
           _activity_geom
      FROM bioprotect.pad pad
     WHERE pad.activitytitle = _activity_title
       AND pad.rppscore > 0;

    GET DIAGNOSTICS _count = ROW_COUNT;
    RETURN _count;
END;
$function$

