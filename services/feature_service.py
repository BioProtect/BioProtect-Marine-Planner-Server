# services/feature_service.py
import pandas as pd


async def get_feature_data(pg, oid):
    """Retrieve feature data from PostGIS.

    Args:
        pg (PostGIS): An instance of the PostGIS database class.
        oid (string): The feature oid in PostGIS.

    Returns:
        pd.DataFrame: A DataFrame containing the feature data.
    """
    query = """
    SELECT
        oid::integer as id,
        feature_class_name,
        alias,
        description,
        _area as area,
        extent,
        to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text AS creation_date,
        tilesetid,
        source,
        created_by
    FROM marxan.metadata_interest_features
    WHERE oid = %s;
    """
    # Execute the query and return the results as a DataFrame
    return await pg.execute(query, data=[oid], return_format="DataFrame")
