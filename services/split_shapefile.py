import geopandas as gpd
import os


def split_shapefile_by_name(shpefile_path, output_dir):
    """
    Splits a shapefile into multiple shapefiles based on the 'name' field.
    Each output shapefile is named after the corresponding 'name' value.
    """
    gdf = gpd.read_file(shpefile_path)

    os.makedirs(output_dir, exist_ok=True)

    # Loop and save each feature using its `name` field
    for idx, row in gdf.iterrows():
        print('row: ', row)
        area_name = str(row["OrigName"]).replace(
            " ", "_").lower()  # sanitize name
        output_path = os.path.join(output_dir, f"{area_name}.shp")

        try:
            single_gdf = gpd.GeoDataFrame([row], crs=gdf.crs)
            single_gdf.to_file(output_path)
            print(f"✅ Saved: {output_path}")

        except Exception as e:
            print(f"⚠️ Skipping feature at index {idx} due to error: {e}")
            continue


if __name__ == "__main__":
    split_shapefile_by_name(
        "../../MVP/Habitats/Kilkieran_Habitats.shp", "../data/kilkieran_habitats")
