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
    if resolution <= 6:
        return "basin"
    elif resolution == 7:
        return "regional"
    else:
        return "local"


def gen_grid_multi_zoom(shapefile, resolution=7):
    # Load ICES shapefile
    df = gpd.read_file(shapefile)

    print(f"Generating for resolution {resolution}")
    scale_level = get_scale_level(resolution)

    for _, row in df.iterrows():
        name = row['Ecoregion']
        geometry = row['geometry']

        # Normalize to handle both Polygon and MultiPolygon
        geoms = [geometry] if geometry.geom_type == 'Polygon' else list(
            geometry.geoms)

        records = []

        # Process each polygon part
        for geom in geoms:
            hexes = h3.geo_to_cells(geom.__geo_interface__, resolution)
            print(f"{name} (res {resolution}): {len(hexes)} hexes")

            for h in hexes:
                boundary = h3.cell_to_boundary(h)
                lnglats = [(lon, lat)
                           for lat, lon in boundary]  # flip for Shapely
                poly = Polygon(lnglats)
                records.append({
                    'h3_index': h,
                    'resolution': resolution,
                    'scale_level': scale_level,
                    'project_area': name,
                    'geometry': poly,
                    'cost': 1.0,
                    'status': 0
                })

        if records:
            gdf_out = gpd.GeoDataFrame(
                records, geometry='geometry', crs='EPSG:4326')
            print(
                f"📝 Inserting {len(gdf_out)} H3 cells for {name} at res {resolution}")
            gdf_out.to_postgis(
                'h3_cells',
                engine,
                schema='bioprotect',
                if_exists='append',
                index=False
            )


if __name__ == "__main__":
    gen_grid_multi_zoom(
        "./data/shapefiles/ICES_ecoregions/ICES_ecoregions_20171207_erase_ESRI.shp")
