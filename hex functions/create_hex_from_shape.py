import math
import pandas as pd
import geopandas as gpd
from shapely.wkt import dumps, loads
import fiona
from shapely.geometry import Polygon, mapping
import matplotlib.pyplot as plt
from descartes import PolygonPatch
import pyproj
from pyproj import Geod
from shapely.geometry import LineString, Point, Polygon


def generate_angles(orientation="flat"):
    """
    generate the angles for the hexes
    Args:
        orientation (str, optional): determine orientation. 'flat' or 'pointy'. Defaults to "flat".
    Raises:
        Exception: Orientation must be flat or pointy
    Returns:
        list: list of angles in radians
    """
    angles = []
    if orientation == 'flat':
        for i in range(0, 6):
            angles.append((1/3)*math.pi*i)
    elif orientation == 'pointy':
        for i in range(0, 6):
            angles.append((1/3)*math.pi*i + 1/6*math.pi)
    else:
        raise Exception("orientation should be 'flat' or 'pointy'")
    return angles


def hex_corners(center_x, center_y, r, angles):
    """
    Create hex polygon
    Args:
        center_x (float): X co-ord of center
        center_y (float): Y co-ord of center
        r (float): radius of hex 
        angles (list): list of angles
    Returns:
        shapely polygon: shapely polygon
    """
    coords = []
    for angle in angles:
        x = center_x + r * math.cos(angle)
        y = center_y + r * math.sin(angle)
        coords.append((x, y))
    return Polygon(coords)


def create_grid(r=1,
                orientation="pointy",
                offset_coordinates="odd_r",
                clipping=True,
                x_min=-180,
                x_max=180,
                y_min=-90,
                y_max=90):
    """
    Create a Hex grid. CRS agnostic 
    orientattion and offset - https://www.redblobgames.com/grids/hexagons/

    Args:
        r (float, optional): radius in arc degrees. Defaults to 1.
        orientation (str, optional): orientation/type of hex flat at top or pointy at top. Defaults to "pointy".
        offset_coordinates (str): Numbering of polygons of type. 'odd_r'/'even_r' for pointy. odd_q/even_q for flat. Defaults to "odd_r".
        clipping (bool): Clip to extent. Defaults to True.
        x_min (float): minimum longitude. Defaults to -180.
        x_max (float): maximum longitude. Defaults to 180.
        y_min (float): minimum latitude. Defaults to -90.
        y_max (float): maximum latitude. Defaults to 90.

    Raises:
        Exception: only supports pointy odd_r

    Returns:
        geoDataframe: geopandas geodataframe with polygons and metadata
    """

    flat_topped_angles = generate_angles(orientation="flat")
    pointy_topped_angles = generate_angles(orientation="pointy")

    # point to point and side to side diameters and extent
    dia_major = 2*r
    dia_minor = math.sqrt(3) * r
    extent_polygon = Polygon(((x_min, y_min),
                              (x_min, y_max),
                              (x_max, y_max),
                              (x_max, y_min)))

    if orientation == "pointy" and offset_coordinates == "odd_r":
        y = y_min
        list_of_y_coords = []
        while y < y_max + (3/4)*dia_major:
            list_of_y_coords.append(y)
            y = y + (3/4)*dia_major

        centres = []
        for row_number, y in enumerate(list_of_y_coords):
            col_number = 0
            if row_number % 2 == 0:
                # even row, no horizontal shift
                x = x_min
                while x < x_max+dia_minor:
                    polygon = hex_corners(x, y, r, pointy_topped_angles)

                    if clipping:
                        polygon = polygon.intersection(extent_polygon)

                    feature = {"row_number": row_number, "col_number": col_number,
                               "x": x, "y": y, "polygon": polygon}
                    centres.append(feature)
                    x = x + dia_minor
                    col_number += 1
            else:
                # odd row, shift right with 1/2 dia_minor
                x = x_min + (1/2)*dia_minor
                while x < x_max+dia_minor:
                    polygon = hex_corners(x, y, r, pointy_topped_angles)

                    if clipping:
                        polygon = polygon.intersection(extent_polygon)

                    feature = {"row_number": row_number,
                               "col_number": col_number,
                               "x": x,
                               "y": y,
                               "polygon": polygon}
                    centres.append(feature)
                    x = x + dia_minor
                    col_number += 1
        gdf = gpd.GeoDataFrame(centres, geometry="polygon")
    else:
        raise Exception("currently only supports 'pointy and 'odd_r'")
    return gdf


def r_finder_minor(num_hex, dist_min, dist_max):
    """
    Get radius based on distance and no of polys stacked on minor axis 
    horizontal for pointy, vertical for flat

    Args:
        num_hex (int): no of hex to fit over minor axis
        dist_min (float): min distance
        dist_max (float): max distance

    Returns:
        float: r - radius of hex (center-corner)
    """
    dia_minor = ((dist_max - dist_min) / float(num_hex))
    print('(dist_max - dist_min): ', (dist_max - dist_min))
    print('dia_minor: ', dia_minor)
    return dia_minor/math.sqrt(3)


df = gpd.read_file('data/shapefiles/case_study/12/12.shp')
reprojected_df = df.to_crs(epsg=4326)
x_min, y_min, x_max, y_max = reprojected_df.geometry.total_bounds
extent_polygon = Polygon(((x_min, y_min),
                          (x_min, y_max),
                          (x_max, y_max),
                          (x_max, y_min)))

geod = Geod(ellps="WGS84")
poly_area, poly_perimeter = geod.geometry_area_perimeter(extent_polygon)
poly_area_km2 = (poly_area/1e6)
number_of_polygons = int((abs(poly_area_km2))/10000)
print('number_of_polygons: ', number_of_polygons)
# n = 36
r = r_finder_minor(number_of_polygons, x_min, x_max)

gdf = create_grid(r=r,
                  orientation='pointy',
                  offset_coordinates='odd_r',
                  clipping=True,
                  x_min=x_min,
                  x_max=x_max,
                  y_min=y_min,
                  y_max=y_max)
gdf.to_file('data/test/letsgo.shp')
gdf = gdf.to_crs(epsg=4326)
print('gdf: ', list(gdf))
print('gdf: ', gdf.head())

df2 = gpd.read_file('data/shapefiles/case_study/12/12.shp')
reprojected_df2 = df2.to_crs(epsg=4326)
points_clip = gpd.clip(gdf, reprojected_df2)
points_clip.to_file('data/test/clipped_letgo.shp')


print('reprojected_df2: ', list(reprojected_df2))
print('reprojected_df2: ', reprojected_df2.head())
# open clip layer
aoi = fiona.open('data/shapefiles/case_study/12/12.shp')
aoiGeom = Polygon(aoi[0]['geometry']['coordinates'][0])
print('aoiGeom: ', aoiGeom)


polyShp = fiona.open('data/test/clipped_letgo.shp')
polyList = []
polyProperties = []
for poly in polyShp:
    polyGeom = Polygon(poly['geometry']['coordinates'][0])
    polyList.append(polyGeom)
    polyProperties.append(poly['properties'])
print(polyList[0])
print(polyProperties[0])

clipPolyList = []
clipPolyProperties = []
for index, poly in enumerate(polyList):
    result = aoiGeom.intersection(poly)
    if result.area:
        clipPolyList.append(result)
        clipPolyProperties.append(polyProperties[index])
print(clipPolyList[0])
print(clipPolyProperties[0])

schema = polyShp.schema

outFile = fiona.open('data/test/clipped_letgo2.shp',
                     mode='w', driver='ESRI Shapefile', schema=schema)
for index, poly in enumerate(clipPolyList):
    outFile.write({
        'geometry': mapping(poly),
        'properties': clipPolyProperties[index]
    })
outFile.close()
