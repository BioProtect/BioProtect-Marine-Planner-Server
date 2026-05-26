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
    -- Uses hex centroid containment instead of polygon intersection
    -- for performance. A hex is "covered" (coverage=1) if its centroid
    -- falls within the activity geometry, otherwise 0.
    --
    -- All pressures for a given activity share the same geometry, so
    -- we test containment once per (activity, hex) then fan out.
    --
    -- Costs are rescaled into [_floor, 1] so no hex ever stores 0.
    -- ---------------------------------------------------------------
    WITH project_hexes AS (
        SELECT pp.id   AS project_pu_id,
               pp.h3_index,
               ST_Centroid(hc.geometry) AS hex_centroid
          FROM bioprotect.project_pus pp
          JOIN bioprotect.h3_cells hc
            ON hc.h3_index = pp.h3_index
         WHERE pp.project_id = _project_id
    ),
    -- Distinct activity geometries, simplified for performance
    activity_geoms AS (
        SELECT DISTINCT ON (activity_id)
               activity_id,
               ST_Simplify(geometry, 0.0001) AS geometry
          FROM bioprotect.pressures
         WHERE activity_id = ANY(_activity_ids)
    ),
    -- Binary containment: 1 if hex centroid is inside activity, else 0
    hex_activity_coverage AS (
        SELECT ph.project_pu_id,
               ph.h3_index,
               ag.activity_id,
               1::numeric AS pressure_coverage
          FROM project_hexes ph
          JOIN activity_geoms ag
            ON ST_Contains(ag.geometry, ph.hex_centroid)
    ),
    -- Fan out to pressures (no spatial ops)
    hex_pressures AS (
        SELECT hac.project_pu_id,
               hac.h3_index,
               p.pressuretitle,
               p.rppscore,
               hac.pressure_coverage
          FROM hex_activity_coverage hac
          JOIN bioprotect.pressures p
            ON p.activity_id = hac.activity_id
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
                   hp.pressure_coverage
                   * hp.rppscore
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

    RETURN _cost_profile;
END;
$$;
