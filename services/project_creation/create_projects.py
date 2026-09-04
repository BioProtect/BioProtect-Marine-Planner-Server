from sqlalchemy import create_engine, text
from geoalchemy2 import Geometry
from classes.db_config import DBConfig
from datetime import datetime
import geopandas as gpd
import pandas as pd

# Load DB config and connect
config = DBConfig()
db_url = (
    f"postgresql://{config.DATABASE_USER}:"
    f"{config.DATABASE_PASSWORD}@"
    f"{config.DATABASE_HOST}/{config.DATABASE_NAME}"
)
engine = create_engine(db_url)

# ✅ Get first user ID from database
with engine.connect() as conn:
    result = conn.execute(
        text("SELECT id FROM bioprotect.users ORDER BY id ASC LIMIT 1"))
    first_user = result.fetchone()
    if not first_user:
        raise ValueError("❌ No users found in the database.")
    user_id = first_user[0]


# Load all H3 cells into a GeoDataFrame
gdf = gpd.read_postgis("""
    SELECT * FROM bioprotect.h3_cells WHERE scale_level = 'regional'
""", engine, geom_col='geometry')

# Get all unique ICES regions
regions = gdf["project_area"].unique()

for region in regions:
    safe_name = region.lower().replace(" ", "_").replace("-", "_").replace("/", "_")
    pu_table = f"pu_{safe_name}"
    print(f"Creating project for: {region}")

    # Subset the GeoDataFrame
    subset = gdf[gdf["project_area"] == region].copy()
    subset["puid"] = range(1, len(subset) + 1)
    subset = subset[["puid", "geometry"]]

    # 1. Create planning unit table using GeoPandas
    subset.to_postgis(pu_table, engine, schema="bioprotect",
                      if_exists="replace", index=False)

    # 2. Compute metadata
    area_km2 = subset.to_crs(6933).geometry.area.sum() / 1_000_000
    envelope = subset.geometry.unary_union.envelope
    if envelope.geom_type == "MultiPolygon":
        envelope = max(envelope.geoms, key=lambda g: g.area)

    metadata_df = pd.DataFrame([{
        "feature_class_name": pu_table,
        "alias": region,
        "description": f"Planning units for {region}",
        "domain": "marine",
        "_area": area_km2,
        "envelope": envelope,
        "creation_date": datetime.utcnow(),
        "source": "ICES ecoregions",
        "created_by": "system",
        "planning_unit_count": len(subset)
    }])

    metadata_gdf = gpd.GeoDataFrame(
        metadata_df, geometry="envelope", crs="EPSG:4326")
    metadata_gdf.to_postgis("metadata_planning_units", engine,
                            schema="bioprotect", if_exists="append", index=False)

    # 3. Get the inserted planning_unit_id
    pu_query = f"""
        SELECT unique_id FROM bioprotect.metadata_planning_units
        WHERE feature_class_name = '{pu_table}'
    """
    planning_unit_id = pd.read_sql(pu_query, engine).iloc[0]['unique_id']

    # 4. Insert into projects
    project_df = pd.DataFrame([{
        'user_id': user_id,
        'name': region,
        'description': f'Project for ICES region {region}',
        'date_created': datetime.utcnow(),
        'planning_unit_id': planning_unit_id,
        'old_version': None,
        'iucn_category': None,
        'is_private': False,
        'costs': 'Equal Area',
        'default_resolution': 7
    }])
    project_df.to_sql("projects", engine, schema="bioprotect",
                      if_exists="append", index=False)

    project_id = pd.read_sql(
        f"""SELECT id FROM bioprotect.projects
              WHERE name = '{region}' AND planning_unit_id = {planning_unit_id}
              ORDER BY id DESC LIMIT 1""", engine
    ).iloc[0]['id']

    # 5. Link to user_projects
    pd.DataFrame([{'user_id': user_id, 'project_id': project_id}]).to_sql(
        'user_projects', engine, schema='bioprotect', if_exists='append', index=False)

    print(
        f"  ✔ Project '{region}' created with planning unit table '{pu_table}'")
