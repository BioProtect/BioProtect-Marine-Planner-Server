import h3
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd
from classes.db_config import DBConfig
from sqlalchemy import create_engine
from datetime import datetime

# Setup database connection
db_config = DBConfig()
db_url = (
    f"postgresql://{db_config.DATABASE_USER}:"
    f"{db_config.DATABASE_PASSWORD}@"
    f"{db_config.DATABASE_HOST}/"
    f"{db_config.DATABASE_NAME}"
)
engine = create_engine(db_url)


def insert_single_area(shapefile_path, project_area_name, resolution=8):
    """
    Inserts H3 cells for a single shapefile and project area at a fixed resolution.

    Args:
        shapefile_path (str): Path to the input shapefile.
        project_area_name (str): Name to use for project_area field in h3_cells.
        resolution (int): H3 resolution (default = 8).
    """
    print(f"\n📍 Processing: {project_area_name} (res {resolution})")

    # Load shapefile into GeoDataFrame
    df = gpd.read_file(shapefile_path)

    for _, row in df.iterrows():
        geometry = row["geometry"]

        # Normalize polygons (handle both Polygon and MultiPolygon)
        geoms = [geometry] if geometry.geom_type == "Polygon" else list(
            geometry.geoms)

        records = []

        # Process each polygon part
        for geom in geoms:
            hexes = h3.geo_to_cells(geom.__geo_interface__, resolution)
            print(f"{project_area_name} (res {resolution}): {len(hexes)} hexes")

            for h in hexes:
                boundary = h3.cell_to_boundary(h)
                lnglats = [(lon, lat)
                           for lat, lon in boundary]  # flip for Shapely
                poly = Polygon(lnglats)
                records.append({
                    'h3_index': h,
                    'resolution': resolution,
                    'scale_level': "local",
                    'project_area': project_area_name,
                    'geometry': poly,
                    'cost': 1.0,
                    'status': 0
                })

        if records:
            gdf_out = gpd.GeoDataFrame(
                records, geometry='geometry', crs='EPSG:4326')
            print(
                f"📝 Inserting {len(gdf_out)} H3 cells for {project_area_name} at res {resolution}")
            gdf_out.to_postgis(
                'h3_cells',
                engine,
                schema='bioprotect',
                if_exists='append',
                index=False
            )


if __name__ == "__main__":
    # Example calls
    insert_single_area("./data/tralee_bay.shp", "Tralee Bay")
    insert_single_area("./data/dunmore_east.shp", "Dunmore East")
