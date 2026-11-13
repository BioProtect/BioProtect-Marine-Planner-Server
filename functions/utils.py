from pathlib import Path

import numpy as np
import rasterio
from classes.db_config import get_db_config
from classes.folder_path_config import get_folder_path_config
from pyproj import CRS
from rasterio.warp import Resampling, calculate_default_transform, reproject

# Get the directory of the current file
dir_path = Path(__file__).resolve().parent
# Get the parent directory
data_path = dir_path.parent
folder_path_config = get_folder_path_config()
db_config = get_db_config()


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


wgs84 = CRS.from_proj4(folder_path_config.gis_config.get("wgs84_str"))
JOSE_CRS = CRS.from_proj4(folder_path_config.gis_config.get("jose_crs_str"))
engine = db_config.engine
