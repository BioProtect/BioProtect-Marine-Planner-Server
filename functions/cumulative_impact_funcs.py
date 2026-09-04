import csv
import json
import subprocess
from subprocess import PIPE, CalledProcessError, Popen

import fiona
import geopandas as gpd
import psycopg2
import rasterio
import rasterio.mask
from psycopg2.sql import SQL, Identifier
from pyproj import CRS
from rasterio import Affine, features
from rasterio.crs import CRS as rasteriocrs
from rasterio.io import MemoryFile
from rasterio.transform import from_bounds
from rasterio.warp import Resampling, calculate_default_transform, reproject
from sqlalchemy import create_engine, exc
from operator import itemgetter

wgs84 = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
conn = psycopg2.connect(
    'postgresql://postgres:oxen4chit@localhost/marxanserver')
cur = conn.cursor()


def get_activity_rasters(activityIds):
    print('activityIds: ', activityIds)
    data = []
    query = "SELECT activity, activity_name, extent FROM bioprotect.metadata_activities WHERE id = ANY(%s)"
    cur.execute(query, (activityIds,))
    result = cur.fetchall()
    print('result: ', result)
    for res in result:
        data.append({
            "activity": res[0],
            "activity_name": res[1],
            "extent": res[2]
        })
    return data

    # for id in activityIds:
    #     pg.execute(
    #         sql.SQL(
    #             "CREATE TABLE bioprotect.{} AS SELECT bioprotect.ST_SplitAtDateLine(ST_SetSRID(ST_MakePolygon(%s)::geometry, 4326)) AS geometry;"
    #         ).format(
    #             sql.Identifier(feature_class_name)
    #         ),
    #         [self.get_argument('linestring')]
    #     )
    """
        SELECT activity, activity_name, extent, rast(SELECT rast FROM bioprotect.activity_name) AS rast FROM bioprotect.metadata_activities WHERE id IN (activityIds)
    """
    return


def get_raster(activity_name):
    raster = cur.execute(
        SQL("SELECT rast from bioprotect.{}").format(Identifier(activity_name)))
    return cur.fetchone()


def get_pad(activity):
    pad_data = cur.execute(
        "select activitytitle, pressuretitle, rppscore from bioprotect.pad WHERE pad.activitytitle = %s;", (activity,))
    return cur.fetchall()


def create_pressures(activity_dict, rast):
    pad = get_pad(activity_dict['activity'])
    format_act = activity_dict['activity'].replace(" ", "_").lower()
    print('format_act: ', format_act)
    for pressure in pad:
        print('pressure: ', pressure[1], " : ", pressure[2])
        score = int(pressure[2])
        if score > 0:
            cur.execute(
                "select exists(select 1 from bioprotect.temp_pressures where pressure=%s)", [pressure[1]])
            if cur.fetchall():
                print('exists')
            else:
                print('doesnt exist')
            cur.execute(SQL(
                "INSERT INTO bioprotect.temp_pressures (pressure, rast, activity) VALUES (%s, bioprotect.ST_MapAlgebra(%s, 1, NULL, '[rast] * %s'), %s)"
            ).format(Identifier(activity_dict['activity_name'])),
                [pressure[1], rast, score, format_act])
            conn.commit()
    return


def close():
    cur.close()
    conn.close()


def postgis_to_shape(table_name, filename):
    sql = "select * from bioprotect.%s;" % table_name
    df = gpd.read_postgis(sql, conn, geom_col='geometry')
    print('df: ', df)
    df.to_file(filename)


def crop_rast_by_shp(shp_path, raster, out_file):
    try:
        cmds = "gdalwarp -cutline " + shp_path + " " + raster + " " + out_file
        subprocess.call(cmds, shell=True)
    except TypeError as e:
        print('e: ', e)


def reproject_raster(file, output_file, reprojection_crs='EPSG:4326'):
    with rasterio.open(file, 'r+') as src:
        transform, width, height = calculate_default_transform(src.crs,
                                                               reprojection_crs,
                                                               src.width,
                                                               src.height,
                                                               *src.bounds)
        with rasterio.open(output_file, 'w', **src.meta) as dst:
            for i in range(1, src.count + 1):
                reproject(source=rasterio.band(src, i),
                          destination=rasterio.band(dst, i),
                          src_transform=src.transform,
                          width=width,
                          height=height,
                          src_crs=src.crs,
                          dst_transform=transform,
                          dst_crs=reprojection_crs,
                          resampling=Resampling.bilinear)
    return output_file


# wgs84_rast = reproject_raster(file='data/uploaded_rasters/gdal.tif',
#                               output_file='data/uploaded_rasters/repro_gdal.tif')
# crop_rast_by_shp(shp_path='data/shapefiles/case_study/12/12.shp',
#                  raster='data/uploaded_rasters/impact.tif',
#                  out_file='data/uploaded_rasters/croppedjose_impact.tif')
# reproject_raster(file='data/uploaded_rasters/croppedjose_impact.tif',
#                  output_file='data/uploaded_rasters/wgs84.tif')
# impact_cost()
# crop_rast_by_shp('data/shapefiles/case_study/12/12.shp',
#                  'data/uploaded_rasters/impact.tif', 'data/uploaded_rasters/impact_cropped.tif')
# print('check')


# activities = get_activity_rasters([10])
# print('activities: ', activities)
# for act in activities:
#     print('act: ', act)
#     rast = get_raster(act['activity_name'])
#     print('act: ', act)
#     create_pressures(act, rast)
# shape_to_hex()
# postgis_to_shape('pu_8cd0be9812474bb6b03e2826ebd29', 'data/tmp/atlas12.shp')
# create_impact_cost_file('pu_8cd0be9812474bb6b03e2826ebd29',
    # 'data/tmp/atlas12.shp')

# reproject_raster(file='data/tmp/impact.tif',
#                  output_file='data/uploaded_rasters/impact2.tif')
def reproject_shape(filename, save_path, reproject):
    """
    reproject a shapefile from one CRS to another.

    Args:
        filename (shapefile): shapefile to be reprojected
        reproject (CRS): CRS from CRS.from_proj4() function
        save_path (string): location of where to save file

    Returns:
        string: returns the location of the newly reprojected shapefile
    """
    shapefile = gpd.read_file(filename)
    shapefile = shapefile.to_crs(reproject)
    shapefile.to_file(save_path)
    return save_path


def shapefile_to_shapefiles(filename, base_folder, shapefile_splitter):
    with fiona.open(filename, "r") as shapefile:
        meta = shapefile.meta
        meta['schema']['properties']['puid'] = 'int'
        for idx, val in enumerate(shapefile):
            filename = base_folder + \
                str(val['properties'].get(shapefile_splitter))
            val['properties']['puid'] = None
            with fiona.open(filename, 'w', **meta, ) as dst:
                dst.write(val)


wgs84 = CRS.from_proj4("+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs")
wgs84_all_habs = reproject_shape('./data/shapefiles/all_habitats.shp',
                                 './data/shapefiles/all_habs.shp',
                                 wgs84)
wgs_all_habs = shapefile_to_shapefiles(wgs84_all_habs,
                                       './data/shapefiles/all_habs/',
                                       'EUNIScombD')
