CREATE OR REPLACE FUNCTION bioprotect.run_cumulative_impact(
    _project_id    INTEGER,
    _activity_ids  INTEGER[],
    _profile_name  TEXT,
    _description   TEXT DEFAULT '',
    _user          TEXT DEFAULT 'system',
    _floor         NUMERIC DEFAULT 0.001
)
RETURNS INTEGER  -- cost_profile_id
LANGUAGE plpgsql
AS $$
DECLARE
    _cost_profile INTEGER;
    _act          RECORD;
BEGIN
    -- ---------------------------------------------------------------
    -- Validate floor: cells must never have cost <= 0 because
    -- Prioritizr would always favour them.
    -- ---------------------------------------------------------------
    IF _floor IS NULL OR _floor <= 0 OR _floor >= 1 THEN
        RAISE EXCEPTION 'floor must be strictly between 0 and 1';
    END IF;

    -- ---------------------------------------------------------------
    -- Validate project has planning units
    -- ---------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1 FROM bioprotect.project_pus WHERE project_id = _project_id
    ) THEN
        RAISE EXCEPTION 'Project % has no planning units', _project_id;
    END IF;

    -- ---------------------------------------------------------------
    -- Create the cost profile
    -- ---------------------------------------------------------------
    INSERT INTO bioprotect.cost_profiles
        (project_id, name, description, created_by, is_default)
    VALUES
        (_project_id, _profile_name, _description, _user, false)
    RETURNING id INTO _cost_profile;

    -- ---------------------------------------------------------------
    -- Record which activities make up this cost profile so the UI can
    -- list and visualise them later.
    -- ---------------------------------------------------------------
    INSERT INTO bioprotect.cost_profile_activities (cost_profile_id, activity_id)
    SELECT _cost_profile, aid
      FROM unnest(_activity_ids) AS aid
     WHERE EXISTS (
         SELECT 1 FROM bioprotect.metadata_activities ma WHERE ma.id = aid
     );

    -- ---------------------------------------------------------------
    -- Compute cumulative impact per hex and insert as cost values.
    --
    -- A hex is "covered" by an activity when its centroid lies in any of
    -- the activity's own polygons, tested through that table's spatial
    -- index - no union, no simplify, so huge activities cost no more than
    -- the cells they touch. Each covered (hex, activity) then fans out to
    -- the activity's pressures straight from PAD (rppscore > 0), so PAD
    -- edits apply to the next run. (This replaced a `pressures` table that
    -- held a merged copy of the footprint per PAD row; dropped in 0018.)
    --
    -- Costs are rescaled into [_floor, 1] so no hex ever stores 0.
    -- ---------------------------------------------------------------
    DROP TABLE IF EXISTS pg_temp.ci_hexes, pg_temp.ci_coverage;
    CREATE TEMP TABLE ci_hexes ON COMMIT DROP AS
    SELECT pp.id AS project_pu_id,
           pp.h3_index,
           ST_Centroid(hc.geometry) AS hex_centroid
      FROM bioprotect.project_pus pp
      -- One geometry per cell: overlapping grids repeat h3_index in
      -- h3_cells, which would multiply every hex's impact.
      CROSS JOIN LATERAL (
        SELECT h.geometry FROM bioprotect.h3_cells h
         WHERE h.h3_index = pp.h3_index
         LIMIT 1
      ) hc
     WHERE pp.project_id = _project_id;
    CREATE INDEX ON ci_hexes USING gist (hex_centroid);
    ANALYZE ci_hexes;

    CREATE TEMP TABLE ci_coverage (
        project_pu_id INTEGER,
        h3_index      TEXT,
        activity_id   INTEGER
    ) ON COMMIT DROP;

    -- Driven from the activity's polygons into the indexed centroids, so
    -- each (possibly 100k-vertex) polygon is prepared once and probed many
    -- times. Hex-driven EXISTS was 60x slower on a 192k-cell grid.
    -- ST_Intersects, not ST_Contains: activities come as several polygons
    -- (raster pieces, shapefile parts), and a centroid on a shared edge is
    -- inside the activity even though no single polygon "contains" it.
    FOR _act IN
        SELECT ma.id, ma.activity_name
          FROM bioprotect.metadata_activities ma
         WHERE ma.id = ANY(_activity_ids)
           AND ma.activity_name IS NOT NULL
    LOOP
        EXECUTE format($f$
            INSERT INTO ci_coverage (project_pu_id, h3_index, activity_id)
            SELECT DISTINCT h.project_pu_id, h.h3_index, %s
              FROM bioprotect.%I a
              JOIN ci_hexes h ON ST_Intersects(a.geometry, h.hex_centroid)
        $f$, _act.id, _act.activity_name);
    END LOOP;

    WITH hex_pressures AS (
        SELECT cov.project_pu_id,
               cov.h3_index,
               pad.pressuretitle,
               pad.rppscore
          FROM ci_coverage cov
          JOIN bioprotect.metadata_activities ma ON ma.id = cov.activity_id
          JOIN bioprotect.pad pad
            ON pad.activitytitle = ma.activity
           AND pad.rppscore > 0
    ),
    hex_features AS (
        SELECT pfa.h3_index,
               mif.alias AS feature_name,
               pfa.amount AS feature_coverage
          FROM bioprotect.pu_feature_amounts pfa
          JOIN bioprotect.metadata_interest_features mif
            ON mif.unique_id = pfa.feature_unique_id
         WHERE pfa.project_id = _project_id
           AND pfa.amount > 0
    ),
    cumulative AS (
        SELECT hp.project_pu_id,
               SUM(
                   hp.rppscore
                   * hf.feature_coverage
                   * COALESCE(sm.sensitivity_score, 0)
               ) AS impact
          FROM hex_pressures hp
          JOIN hex_features hf
            ON hp.h3_index = hf.h3_index
          LEFT JOIN bioprotect.sensitivity_matrix sm
            ON sm.eunis_code  = hf.feature_name
           AND sm.pressure    = hp.pressuretitle
         GROUP BY hp.project_pu_id
    )
    -- Insert impacted hexes with normalised cost in [_floor, 1].
    -- Linear remap from [0, 1] into [_floor, 1] so the lowest impact
    -- still lands on _floor, never 0.
    INSERT INTO bioprotect.cost_profile_values
        (cost_profile_id, project_pu_id, cost, status)
    SELECT _cost_profile,
           c.project_pu_id,
           CASE
             WHEN max_impact.val > 0
             THEN GREATEST(
                    _floor,
                    LEAST(
                        1.0,
                        _floor + (1.0 - _floor) * (c.impact / max_impact.val)
                    )
                  )
             ELSE _floor
           END,
           0
      FROM cumulative c,
           (SELECT MAX(impact) AS val FROM cumulative) max_impact;

    -- Fill non-impacted hexes with the floor cost (not zero).
    INSERT INTO bioprotect.cost_profile_values
        (cost_profile_id, project_pu_id, cost, status)
    SELECT _cost_profile, pp.id, _floor, 0
      FROM bioprotect.project_pus pp
     WHERE pp.project_id = _project_id
       AND NOT EXISTS (
           SELECT 1 FROM bioprotect.cost_profile_values cpv
            WHERE cpv.cost_profile_id = _cost_profile
              AND cpv.project_pu_id   = pp.id
       );

    -- The cost layer is read straight after this; without fresh statistics
    -- the planner assumes the new profile has ~1 row and picks a quadratic join.
    ANALYZE bioprotect.cost_profile_values;

    RETURN _cost_profile;
END;
$$;
