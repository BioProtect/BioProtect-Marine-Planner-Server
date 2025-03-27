get_pu_grids_query = """
        SELECT DISTINCT
            alias,
            feature_class_name,
            description,
            to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date,
            country_id,
            aoi_id,
            domain,
            _area,
            ST_AsText(envelope) AS envelope,
            pu.source,
            original_n AS country,
            created_by,
            tilesetid,
            planning_unit_count
        FROM marxan.metadata_planning_units pu
        LEFT OUTER JOIN marxan.gaul_2015_simplified_1km
        ON id_country = country_id
        ORDER BY alias;
    """
