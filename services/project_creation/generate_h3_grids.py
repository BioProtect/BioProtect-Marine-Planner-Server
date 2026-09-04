import h3
import geopandas as gpd
from shapely.geometry import Polygon
import pandas as pd
from classes.db_config import DBConfig
from sqlalchemy import create_engine, exc

db_config = DBConfig()

db_url = (
    f"postgresql://{db_config.DATABASE_USER}:"
    f"{db_config.DATABASE_PASSWORD}@"
    f"{db_config.DATABASE_HOST}/"
    f"{db_config.DATABASE_NAME}"
)
engine = create_engine(db_url)


def get_scale_level(resolution):
    if resolution <= 2:
        return "basin"
    elif 3 <= resolution <= 5:
        return "regional"
    else:
        return "local"


def gen_grid(shapefile, zoom_lvl):
    # Load ICES shapefile
    df = gpd.read_file(shapefile)
    resolution = zoom_lvl  # or adjust as needed
    records = []
    for _, row in df.iterrows():
        name = row['Ecoregion']
        geometry = row['geometry']

        # Normalize to handle both Polygon and MultiPolygon
        if geometry.geom_type == 'Polygon':
            geoms = [geometry]
        elif geometry.geom_type == 'MultiPolygon':
            geoms = list(geometry.geoms)
        else:
            continue  # Skip unexpected geometry types

        # Process each polygon part
        for geom in geoms:
            hexes = h3.geo_to_cells(geom.__geo_interface__, resolution)
            print(f"{name}: {len(hexes)} hexes")

            for h in hexes:
                boundary = h3.cell_to_boundary(h)
                lnglats = [(lon, lat)
                           for lat, lon in boundary]  # flip for Shapely
                poly = Polygon(lnglats)
                records.append({
                    'h3_index': h,
                    'resolution': resolution,
                    'scale_level': 'regional',
                    'project_area': name,
                    'geometry': poly,
                    'cost': 1.0,
                    'status': 0
                })

    gdf_out = gpd.GeoDataFrame(records, geometry='geometry', crs='EPSG:4326')
    print(f"Inserting {len(gdf_out)} H3 cells into PostGIS...")
    gdf_out.to_postgis(f'h3_cells', engine, schema='bioprotect',
                       if_exists='append', index=False)


if __name__ == "__main__":
    gen_grid(
        "./data/shapefiles/ICES_ecoregions/ICES_ecoregions_20171207_erase_ESRI.shp", 6)
