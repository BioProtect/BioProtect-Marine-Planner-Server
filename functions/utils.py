import csv
import ctypes
import glob
import io
import os
import platform
import re
import string
import subprocess
from operator import itemgetter
from os import pardir, path, sep
from pathlib import Path
from string import punctuation
from subprocess import PIPE

import geopandas as gpd
import numpy as np
import psutil
import rasterio
from classes.db_config import get_db_config
from classes.folder_path_config import get_folder_path_config
from mapbox import Uploader
from osgeo import gdal, gdalconst
from pyproj import CRS
from rasterio.enums import Resampling as ResampleEnum
from rasterio.warp import Resampling, calculate_default_transform, reproject
from services.service_error import ServicesError
from sqlalchemy import create_engine

# Get the directory of the current file
dir_path = Path(__file__).resolve().parent
# Get the parent directory
data_path = dir_path.parent
folder_path_config = get_folder_path_config()
db_config = get_db_config()


def replace_chars(text):
    cleaned = text.translate(str.maketrans("", "", string.punctuation))
    # Replace any sequence of whitespace with a single underscore
    cleaned = re.sub(r"\s+", "_", cleaned)
    return cleaned.lower()


def pad_dict(k, val, width=25):
    """Outputs a key-value pair from a dictionary into a formatted string with specified width.

    Args:
        k (str): The dictionary key.
        v (str): The dictionary value.
        w (int): The width of the key column. The key will be padded to this width.

    Returns:
        str: The formatted key-value pair as a string.
    """
    # Validate the width
    if width < 0:
        raise ValueError("Width must be a non-negative integer.")

    # Pad the key to the specified width and concatenate with the value
    return f"{k:<{width}}{val}"


def get_free_space_gb():
    """Gets the drive free space in gigabytes.

    Args:
        None
    Returns:
        string: The free space in Gb, e.g. 1.2 Gb
    """
    if platform.system() == 'Windows':
        # Get free space on Windows using ctypes
        free_bytes = ctypes.c_ulonglong(0)
        ctypes.windll.kernel32.GetDiskFreeSpaceExW(
            ctypes.c_wchar_p(folder_path_config.PROJECT_FOLDER),
            None, None, ctypes.pointer(free_bytes)
        )
        space_gb = free_bytes.value / (1024 ** 3)  # Convert bytes to gigabytes
    else:
        # Get free space on Unix-like systems
        st = os.statvfs(folder_path_config.PROJECT_FOLDER)
        space_gb = st.f_bavail * st.f_frsize / \
            (1024 ** 3)  # Convert bytes to gigabytes

    return f"{space_gb:.1f} Gb"  # Format and return as a string


def get_tif_list(tif_dir, extension="tif"):
    """Get a list of all the tifs in a directory

    Keyword arguments:
    tif_dir -- the directory where the list of tifs is
    Return: a sorted list of dictionaries containing the filename and path
    """
    directory = Path(tif_dir)
    files = directory.rglob(f"*.{extension}")

    file_list = [{
        "label": file.stem,  # filename without extension
        "path": str(file)
    } for file in files]

    return sorted(file_list, key=lambda i: i["label"])


def setup_sens_matrix():
    print('Setting up sensitivity matrix....')
    habitat_list = [item['label'] for item in
                    get_tif_list('/'+folder_path_config.gis_config['input_coral'], 'asc') +
                    get_tif_list('/'+folder_path_config.gis_config['input_fish'], 'asc')]
    sens_mat = folder_path_config.gis_config['sensmat']
    for habitat_name in habitat_list:
        sens_mat.loc[habitat_name] = sens_mat.loc['VME']
    return sens_mat


def normalize_nparray(nparray):
    masked = np.ma.masked_invalid(nparray)
    logged = np.log(masked + 1)

    min_val = logged.min()
    max_val = logged.max()

    normalized = (logged - min_val) / (max_val - min_val)
    return normalized.filled(0)  # convert masked to 0 where invalid


def get_rasters_transform(rast, reprojection_crs):
    with rasterio.open('data/rasters/all_habitats.tif') as template:
        transform, width, height = calculate_default_transform(
            template.crs, reprojection_crs, template.width, template.height, *template.bounds)
        return {
            "transform": transform,
            "width": width,
            "height": height,
            "meta": template.meta.copy()
        }


def reproject_raster(input_raster, output_folder, reprojection_crs="EPSG:4326", reference_raster=None):
    """
    Reproject a raster to a target CRS or align it exactly to a reference raster.

    Args:
        input_raster (str): Path to the input raster.
        output_folder (str): Folder to save the reprojected raster.
        reprojection_crs (str): Target CRS if not aligning to a reference raster. Defaults to 'EPSG:4326'.
        reference_raster (str, optional): Path to a reference raster to match dimensions, transform, and CRS.

    Returns:
        str: Path to the newly reprojected raster.
    """
    filename = input_raster.split("/")[-1].split(".")[0]
    output_path = f"{output_folder}{filename}.tif"

    with rasterio.open(input_raster) as src:

        if reference_raster:
            # Match target transform and dimensions to reference raster
            with rasterio.open(reference_raster) as ref:
                dst_crs = ref.crs
                dst_transform = ref.transform
                width = ref.width
                height = ref.height
        else:
            # Compute best-fit transform for target CRS
            dst_crs = reprojection_crs
            dst_transform, width, height = calculate_default_transform(
                src.crs, dst_crs, src.width, src.height, *src.bounds
            )

        # Prepare metadata for output raster
        out_meta = src.meta.copy()
        out_meta.update({
            "crs": dst_crs,
            "transform": dst_transform,
            "height": height,
            "width": width,
            "nodata": 0
        })

        with rasterio.open(output_path, "w", **out_meta) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear
                )

    return output_path


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
    print('filename: ', filename)
    shapefile = shapefile.to_crs(reproject)
    shapefile.to_file(save_path / filename.name)
    return save_path


def reproject_and_normalise_upload(raster_name, data, reprojection_crs, wgs84):
    """
    Reprojects and normalizes an uploaded raster to match a common template raster.

    Args:
        raster_name (str): Base name of the raster file (e.g., "input.tif")
        data (DatasetReader): An open rasterio dataset
        reprojection_crs (str): Target CRS for normalization (e.g., EPSG:100026)
        wgs84 (str): CRS string for checking if reprojection to WGS84 is needed

    Returns:
        str: Path to the normalized and aligned raster
    """
    tmp_folder = f"data/tmp/"
    normalized_path = f"{tmp_folder}{raster_name}"

    # Step 1: Reproject to WGS84 if necessary
    if '4326' not in data.crs.to_string():
        data_path = reproject_raster(
            input_path=data.name,
            output_folder=tmp_folder,
            reprojection_crs=wgs84
        )
        rast_data = rasterio.open(data_path)
    else:
        rast_data = data

    # Step 2: Load template metadata to align with
    template_info = get_rasters_transform(
        rast='data/rasters/all_habitats.tif',
        reprojection_crs=reprojection_crs
    )
    meta = template_info['meta'].copy()
    meta.update(nodata=0)

    # Step 3: Normalize and reproject to match the template
    with rasterio.open(normalized_path, "w", **meta) as dst:
        for i in range(1, rast_data.count + 1):
            band = rast_data.read(i)
            band = np.where(band < 0, 0, band)
            norm_band = normalize_nparray(band)

            reproject(
                source=norm_band,
                destination=rasterio.band(dst, i),
                src_transform=rast_data.transform,
                src_crs=rast_data.crs,
                dst_transform=template_info['transform'],
                dst_crs=reprojection_crs,
                resampling=Resampling.bilinear,
                width=template_info['width'],
                height=template_info['height']
            )

    return normalized_path


def reproject_raster_to_all_habs(tmp_path, src_data, src_meta, final_output_path):
    """
    Reprojects a raster to match the 'all_habitats.tif' reference raster.

    Args:
        tmp_path (str): Path to temporary output raster.
        src_data (ndarray): Source raster data to reproject.
        src_meta (dict): Metadata from the source raster.
        final_output_path (str): Path to save the final reprojected raster.
    """
    print('📍 Reprojecting raster to align with all_habitats.tif...')

    # Step 1: Determine source CRS
    src_crs = src_meta.get('crs') or folder_path_config.gis_config['wgs84_str']

    # Step 2: Load template transform and metadata
    template_info = get_rasters_transform(
        rast='data/rasters/all_habitats.tif',
        reprojection_crs=folder_path_config.gis_config['jose_crs_str']
    )
    dst_meta = template_info['meta'].copy()
    dst_meta.update(nodata=src_meta.get('nodata', 0))

    # Step 3: Reproject into aligned raster
    with rasterio.open(tmp_path, "w", **dst_meta) as dst:
        reproject(
            source=src_data,
            destination=rasterio.band(dst, 1),
            src_transform=src_meta['transform'],
            src_crs=src_crs,
            dst_transform=template_info['transform'],
            dst_crs=folder_path_config.gis_config['jose_crs_str'],
            resampling=Resampling.bilinear
        )

    # Step 4: Optionally reproject to template again (if you want exact match on grid)
    reproject_raster(
        input_path=tmp_path,
        output_folder=final_output_path,
        reference_raster='data/rasters/all_habitats.tif'
    )
    return


def create_colormap(min, max):
    colormap = [(124, 202, 247, 255),
                (122, 201, 247, 255),
                (121, 200, 247, 255),
                (120, 199, 247, 255),
                (119, 198, 247, 255),
                (117, 197, 247, 255),
                (116, 195, 247, 255),
                (115, 194, 248, 255),
                (113, 193, 248, 255),
                (112, 192, 248, 255),
                (111, 191, 248, 255),
                (110, 190, 248, 255),
                (108, 188, 248, 255),
                (107, 187, 248, 255),
                (106, 186, 249, 255),
                (104, 185, 249, 255),
                (103, 184, 249, 255),
                (102, 183, 249, 255),
                (101, 181, 249, 255),
                (99, 180, 249, 255),
                (98, 179, 249, 255),
                (97, 178, 250, 255),
                (96, 177, 250, 255),
                (94, 176, 250, 255),
                (93, 174, 250, 255),
                (92, 173, 250, 255),
                (90, 172, 250, 255),
                (89, 171, 250, 255),
                (88, 170, 251, 255),
                (87, 169, 251, 255),
                (85, 167, 251, 255),
                (84, 166, 251, 255),
                (83, 165, 251, 255),
                (81, 164, 251, 255),
                (80, 163, 251, 255),
                (79, 162, 252, 255),
                (78, 160, 252, 255),
                (76, 159, 252, 255),
                (75, 158, 252, 255),
                (74, 157, 252, 255),
                (73, 156, 252, 255),
                (71, 155, 252, 255),
                (70, 153, 253, 255),
                (69, 152, 253, 255),
                (67, 151, 253, 255),
                (66, 150, 253, 255),
                (65, 149, 253, 255),
                (64, 148, 253, 255),
                (62, 146, 254, 255),
                (61, 145, 254, 255),
                (60, 144, 254, 255),
                (58, 143, 254, 255),
                (57, 142, 254, 255),
                (56, 141, 254, 255),
                (55, 139, 254, 255),
                (53, 138, 255, 255),
                (52, 137, 255, 255),
                (51, 136, 255, 255),
                (50, 135, 255, 255),
                (48, 134, 255, 255),
                (47, 132, 255, 255),
                (46, 131, 255, 255),
                (44, 130, 255, 255),
                (43, 129, 255, 255),
                (44, 129, 254, 255),
                (48, 131, 251, 255),
                (51, 133, 248, 255),
                (54, 135, 245, 255),
                (58, 137, 241, 255),
                (61, 139, 238, 255),
                (64, 141, 235, 255),
                (68, 143, 232, 255),
                (71, 145, 228, 255),
                (74, 147, 225, 255),
                (78, 149, 222, 255),
                (81, 151, 219, 255),
                (84, 153, 216, 255),
                (88, 155, 212, 255),
                (91, 157, 209, 255),
                (95, 159, 206, 255),
                (98, 161, 203, 255),
                (101, 163, 200, 255),
                (105, 165, 196, 255),
                (108, 167, 193, 255),
                (111, 169, 190, 255),
                (115, 171, 187, 255),
                (118, 173, 183, 255),
                (121, 175, 180, 255),
                (125, 177, 177, 255),
                (128, 179, 174, 255),
                (132, 181, 171, 255),
                (135, 183, 167, 255),
                (138, 185, 164, 255),
                (142, 188, 161, 255),
                (145, 190, 158, 255),
                (148, 192, 155, 255),
                (152, 194, 151, 255),
                (155, 196, 148, 255),
                (158, 198, 145, 255),
                (162, 200, 142, 255),
                (165, 202, 139, 255),
                (168, 204, 135, 255),
                (172, 206, 132, 255),
                (175, 208, 129, 255),
                (179, 210, 126, 255),
                (182, 212, 122, 255),
                (185, 214, 119, 255),
                (189, 216, 116, 255),
                (192, 218, 113, 255),
                (195, 220, 110, 255),
                (199, 222, 106, 255),
                (202, 224, 103, 255),
                (205, 226, 100, 255),
                (209, 228, 97, 255),
                (212, 230, 94, 255),
                (215, 232, 90, 255),
                (219, 234, 87, 255),
                (222, 236, 84, 255),
                (226, 238, 81, 255),
                (229, 240, 77, 255),
                (232, 242, 74, 255),
                (236, 244, 71, 255),
                (239, 246, 68, 255),
                (242, 248, 65, 255),
                (246, 250, 61, 255),
                (249, 252, 58, 255),
                (252, 254, 55, 255),
                (255, 255, 52, 255),
                (255, 254, 53, 255),
                (255, 252, 54, 255),
                (255, 250, 55, 255),
                (255, 248, 56, 255),
                (255, 246, 57, 255),
                (255, 244, 59, 255),
                (255, 241, 60, 255),
                (255, 239, 61, 255),
                (255, 237, 62, 255),
                (255, 235, 63, 255),
                (255, 233, 64, 255),
                (255, 231, 65, 255),
                (255, 229, 66, 255),
                (255, 227, 68, 255),
                (255, 225, 69, 255),
                (255, 223, 70, 255),
                (255, 221, 71, 255),
                (255, 219, 72, 255),
                (255, 217, 73, 255),
                (255, 215, 74, 255),
                (255, 213, 76, 255),
                (255, 211, 77, 255),
                (255, 209, 78, 255),
                (255, 207, 79, 255),
                (255, 205, 80, 255),
                (255, 203, 81, 255),
                (255, 201, 82, 255),
                (255, 199, 83, 255),
                (255, 196, 85, 255),
                (255, 194, 86, 255),
                (255, 192, 87, 255),
                (255, 190, 88, 255),
                (255, 188, 89, 255),
                (255, 186, 90, 255),
                (255, 184, 91, 255),
                (255, 182, 93, 255),
                (255, 180, 94, 255),
                (255, 178, 95, 255),
                (255, 176, 96, 255),
                (255, 174, 97, 255),
                (255, 172, 98, 255),
                (255, 170, 99, 255),
                (255, 168, 100, 255),
                (255, 166, 102, 255),
                (255, 164, 103, 255),
                (255, 162, 104, 255),
                (255, 160, 105, 255),
                (255, 158, 106, 255),
                (255, 156, 107, 255),
                (255, 154, 108, 255),
                (255, 152, 109, 255),
                (255, 149, 111, 255),
                (255, 147, 112, 255),
                (255, 145, 113, 255),
                (255, 143, 114, 255),
                (255, 141, 115, 255),
                (255, 139, 116, 255),
                (255, 137, 117, 255),
                (255, 135, 119, 255),
                (255, 133, 120, 255),
                (255, 131, 121, 255),
                (255, 129, 122, 255),
                (255, 127, 123, 255),
                (255, 125, 123, 255),
                (255, 124, 121, 255),
                (255, 122, 119, 255),
                (255, 120, 117, 255),
                (255, 119, 115, 255),
                (255, 117, 113, 255),
                (255, 116, 111, 255),
                (255, 114, 109, 255),
                (255, 112, 107, 255),
                (255, 111, 105, 255),
                (255, 109, 103, 255),
                (255, 108, 101, 255),
                (255, 106, 99, 255),
                (255, 104, 97, 255),
                (255, 103, 95, 255),
                (255, 101, 93, 255),
                (255, 100, 92, 255),
                (255, 98, 90, 255),
                (255, 96, 88, 255),
                (255, 95, 86, 255),
                (255, 93, 84, 255),
                (255, 92, 82, 255),
                (255, 90, 80, 255),
                (255, 88, 78, 255),
                (255, 87, 76, 255),
                (255, 85, 74, 255),
                (255, 84, 72, 255),
                (255, 82, 70, 255),
                (255, 81, 68, 255),
                (255, 79, 66, 255),
                (255, 77, 64, 255),
                (255, 76, 62, 255),
                (255, 74, 60, 255),
                (255, 73, 58, 255),
                (255, 71, 56, 255),
                (255, 69, 55, 255),
                (255, 68, 53, 255),
                (255, 66, 51, 255),
                (255, 65, 49, 255),
                (255, 63, 47, 255),
                (255, 61, 45, 255),
                (255, 60, 43, 255),
                (255, 58, 41, 255),
                (255, 57, 39, 255),
                (255, 55, 37, 255),
                (255, 53, 35, 255),
                (255, 52, 33, 255),
                (255, 50, 31, 255),
                (255, 49, 29, 255),
                (255, 47, 27, 255),
                (255, 45, 25, 255),
                (255, 44, 23, 255),
                (255, 42, 21, 255),
                (255, 41, 19, 255),
                (255, 39, 18, 255),
                (255, 37, 16, 255),
                (255, 36, 14, 255),
                (255, 34, 12, 255),
                (255, 33, 10, 255),
                (255, 31, 8, 255),
                (255, 30, 6, 255),
                (255, 28, 4, 255),
                (255, 26, 2, 255),
                (255, 25, 0, 255)]
    raster_colormap = {}
    for val in range(min, max):
        raster_colormap[val] = colormap[val]
    return raster_colormap


def get_layer_cumul_imp(eco_layer, strs_layer, senstivity_val):
    """
    DRY function.
    This is the essense of the cumulative impact function
    get the data for our eco system layers
    get the data for our pressure/stressor layers (should settle on one)
    multiple the eco layer by the pressure layer by the senstivity matrix score

    Keyword arguments:
    eco_layer -- the eco system layer
    strs_layer -- the pressure layer
    senstivity_val -- The sensitivity matrix score for the above 2 layers - how the pressure affects the eco system
    Return: updated matrix of cumulative impacts
    """

    eco = np.around(eco_layer, decimals=4)
    strs = np.around(strs_layer, decimals=4)
    return (np.multiply(eco, strs)) * senstivity_val


def cumul_impact(ecosys_list, sens_mat, stressors_list, nodata_val=0):
    """
    Computes the cumulative ecological impact across ecosystem and stressor layers.

    Args:
        ecosys_list (list[dict]): Each dict contains 'label' and 'path' for ecosystem layers.
        sens_mat (pd.DataFrame): Sensitivity matrix mapping eco-to-stressor scores.
        stressors_list (list[dict]): Each dict contains 'label' and 'path' for stressor layers.
        nodata_val (int or float): Value to treat as 'nodata' in rasters.

    Returns:
        tuple: (normalized_cumulative_impact_array, metadata_dict)
    """
    print("🔁 Running cumulative impact function...")
    cumul_impact = None
    meta = None

    for eco in ecosys_list:
        # Check if the eco system component exists in the sensitivity matrix.
        try:
            eco_row = sens_mat.loc[eco['label']]
        except (ValueError, KeyError):
            continue

        with rasterio.open(eco['path'], "r+") as ecosrc:
            eco_data = ecosrc.read(1)
            if meta is None:
                meta = ecosrc.meta.copy()
                meta.update(nodata=nodata_val)

        for stressor in stressors_list:
            try:
                sens_score = eco_row.loc[stressor['label']]
            except (ValueError, KeyError):
                continue

            if sens_score == 0 or np.isnan(sens_score):
                continue

            with rasterio.open(stressor['path'], 'r+') as stressorrc:
                stressor_data = stressorrc.read(1)
                impact = get_layer_cumul_imp(
                    eco_data, stressor_data, sens_score)

                if cumul_impact is None:
                    cumul_impact = impact
                else:
                    cumul_impact = np.add(cumul_impact, impact)

    normalised_cumul_impact = normalize_nparray(cumul_impact)
    return [normalised_cumul_impact, meta]


def colorise_raster(file_name, outfile_name):
    with rasterio.Env():
        with rasterio.open(file_name) as src:
            shade = src.read(1)
            meta = src.meta

        colors = create_colormap(shade.min(), shade.max())
        with rasterio.open(outfile_name, 'w', **meta) as dst:
            dst.write(shade, indexes=1)
            dst.write_colormap(1, colors)
            cmap = dst.colormap(1)
    return outfile_name


wgs84 = CRS.from_proj4(folder_path_config.gis_config.get("wgs84_str"))
JOSE_CRS = CRS.from_proj4(folder_path_config.gis_config.get("jose_crs_str"))
engine = db_config.engine


def create_cost_from_impact(user, project, pu_tablename, raster_path, impact_type):
    """
    Generates a Marxan-compatible .cost file by intersecting a raster with planning unit centroids.

    Args:
        user (str): Username.
        project (str): Project name.
        pu_tablename (str): Table name of planning units (must include geometry).
        raster_path (str): Path to the raster file.
        impact_type (str): Type of impact (used to name the cost file).
    """
    with rasterio.open(raster_path) as raster:
        band = raster.read(1)
        band = np.clip(band, 0, None)  # Replace negatives with 0
        band_scaled = 100 + 100 * band  # Apply impact-based cost formula

        # Load planning units from PostGIS
        sql = f"SELECT * FROM bioprotect.{pu_tablename};"
        with engine.begin() as conn:
            pu_gdf = gpd.read_postgis(sql, conn, geom_col='geometry')

        # Extract centroids and intersect with raster
        rows = []
        for puid, geom in zip(pu_gdf['puid'], pu_gdf['geometry']):
            centroid = geom.centroid
            row, col = raster.index(centroid.x, centroid.y)
            try:
                cost = band_scaled[row, col]
            except IndexError:
                cost = 0  # Outside raster bounds
            rows.append([puid, cost])

        # Create output folder and write .cost file
        folder = os.path.join("users", user, project, "input", impact_type)
        os.makedirs(os.path.dirname(folder), exist_ok=True)
        output_file = folder + ".cost"

        with open(output_file, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(["id", "cost"])
            writer.writerows(sorted(rows, key=itemgetter(0)))
