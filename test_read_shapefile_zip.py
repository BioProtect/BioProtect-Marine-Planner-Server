"""Self-check for read_shapefile_zip (project-grid shapefile uploads).

    .venv/bin/python -W ignore test_read_shapefile_zip.py
"""

import os
import shutil
import tempfile
import zipfile

import geopandas as gpd
from shapely.geometry import box

from handlers.planning_unit_websocket_handler import read_shapefile_zip
from services.service_error import ServicesError

# ~10 km square off Galway, lat/lon and Irish Transverse Mercator
LATLON = gpd.GeoDataFrame(geometry=[box(-9.3, 53.1, -9.15, 53.2)], crs=4326)
ITM = LATLON.to_crs(2157)


def make_zip(d, gdf, inner="", drop=()):
    """Write gdf as a shapefile, zip it under the folder `inner`, minus `drop` extensions."""
    shp_dir = os.path.join(d, "shp")
    os.makedirs(shp_dir, exist_ok=True)
    gdf.to_file(os.path.join(shp_dir, "area.shp"))
    zp = os.path.join(d, "upload.zip")
    with zipfile.ZipFile(zp, "w") as z:
        for f in os.listdir(shp_dir):
            if os.path.splitext(f)[1] not in drop:
                z.write(os.path.join(shp_dir, f), inner + f)
    shutil.rmtree(shp_dir)
    return zp


def expect_error(zp, text):
    try:
        read_shapefile_zip(zp)
    except ServicesError as e:
        assert text in str(e), str(e)
    else:
        raise AssertionError(f"no error, expected '{text}'")


def bounds_match(df):
    return all(abs(a - b) < 1e-6 for a, b in zip(df.total_bounds, LATLON.total_bounds))


def main():
    with tempfile.TemporaryDirectory() as d:
        # files at the zip root
        assert bounds_match(read_shapefile_zip(make_zip(d, LATLON)))
        # zipped as a folder: the case GDAL reports as "does not exist"
        assert bounds_match(read_shapefile_zip(make_zip(d, LATLON, "My Area/")))
        # projected CRS is reprojected to lat/lon for H3
        assert bounds_match(read_shapefile_zip(make_zip(d, ITM)))
        # no .prj: lat/lon numbers are assumed 4326, metres are refused
        assert bounds_match(read_shapefile_zip(make_zip(d, LATLON, drop=(".prj",))))
        expect_error(make_zip(d, ITM, drop=(".prj",)), "no .prj")
        expect_error(make_zip(d, LATLON, drop=(".shx",)), "no matching .shx")
        expect_error(make_zip(d, LATLON, drop=(".shp",)), "No .shp file")
        bad = os.path.join(d, "bad.zip")
        open(bad, "w").write("not a zip")
        expect_error(bad, "not a valid zip")
    print("ok")


if __name__ == "__main__":
    main()
