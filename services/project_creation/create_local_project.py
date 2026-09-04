import geopandas as gpd
import pandas as pd
from shapely.geometry import box
from sqlalchemy import create_engine
from datetime import datetime
from classes.db_config import DBConfig
# DB setup
config = DBConfig()
db_url = (
    f"postgresql://{config.DATABASE_USER}:"
    f"{config.DATABASE_PASSWORD}@"
    f"{config.DATABASE_HOST}/{config.DATABASE_NAME}"
)
engine = create_engine(db_url)


def create_project_from_shapefile(shapefile_path, user_id, resolution=7, scale_level='regional'):
    # Load shapefile
    area_gdf = gpd.read_file(shapefile_path)
    area_name = area_gdf.iloc[0]["Name"] if "Name" in area_gdf.columns else "custom_area"
    safe_name = area_name.lower().replace(
        " ", "_").replace("-", "_").replace("/", "_")
    pu_table = f"pu_{safe_name}"

    # Calculate bounding box to query only needed H3 cells
    bbox = area_gdf.total_bounds  # [minx, miny, maxx, maxy]
    minx, miny, maxx, maxy = bbox
    bbox_geom = box(minx, miny, maxx, maxy).wkt

    # Load subset of h3_cells
    query = f"""
        SELECT * FROM bioprotect.h3_cells
        WHERE scale_level = '{scale_level}'
          AND resolution = {resolution}
          AND ST_Intersects(geometry, ST_GeomFromText('{bbox_geom}', 4326))
    """
    h3_subset = gpd.read_postgis(query, engine, geom_col='geometry')

    # Spatial filter to exact shape (not just bbox)
    h3_subset = h3_subset[h3_subset.intersects(area_gdf.unary_union)]

    if h3_subset.empty:
        print("⚠️ No H3 cells intersect the given shapefile.")
        return

    # Compute metadata
    area_km2 = h3_subset.to_crs(6933).geometry.area.sum() / 1_000_000
    envelope = h3_subset.geometry.unary_union.envelope

    metadata_df = pd.DataFrame([{
        "feature_class_name": f"v_h3_{safe_name}_res{resolution}",
        "alias": area_name,
        "description": f"Planning units for {area_name}",
        "domain": "marine",
        "_area": area_km2,
        "envelope": envelope,
        "creation_date": datetime.utcnow(),
        "source": "Shapefile input",
        "created_by": "system",
        "planning_unit_count": len(h3_subset)
    }])
    metadata_gdf = gpd.GeoDataFrame(
        metadata_df, geometry="envelope", crs="EPSG:4326")
    metadata_gdf.to_postgis("metadata_planning_units", engine, schema="bioprotect",
                            if_exists="append", index=False)

    # Get planning_unit_id
    planning_unit_id = pd.read_sql(
        f"SELECT unique_id FROM bioprotect.metadata_planning_units WHERE feature_class_name = 'v_h3_{safe_name}_res{resolution}'",
        engine
    ).iloc[0]['unique_id']

    # Insert project
    project_df = pd.DataFrame([{
        'user_id': user_id,
        'name': area_name,
        'description': f'Project for custom region {area_name}',
        'date_created': datetime.utcnow(),
        'planning_unit_id': planning_unit_id,
        'old_version': None,
        'iucn_category': None,
        'is_private': False,
        'costs': 'Equal Area'
    }])
    project_df.to_sql("projects", engine, schema="bioprotect",
                      if_exists="append", index=False)

    # Get project_id
    project_id = pd.read_sql(
        f"""SELECT id FROM bioprotect.projects
            WHERE name = '{area_name}' AND planning_unit_id = {planning_unit_id}
            ORDER BY id DESC LIMIT 1""", engine
    ).iloc[0]['id']

    # Link user to project
    pd.DataFrame([{'user_id': user_id, 'project_id': project_id}]).to_sql(
        'user_projects', engine, schema='bioprotect', if_exists='append', index=False)

    print(f"✅ Project '{area_name}' created using table '{pu_table}'")


if __name__ == "__main__":
    create_project_from_shapefile(
        "./data/tralee_bay.shp", user_id=4, resolution=8, scale_level='local')
    create_project_from_shapefile(
        "./data/dunmore_east.shp", user_id=4, resolution=8, scale_level='local')
