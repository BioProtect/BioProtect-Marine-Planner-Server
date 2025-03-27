import json
import re
import string
from os import path, sep
from pathlib import Path

import pandas as pd
import numpy as np
import geopandas as gpd
import json
from dotenv import dotenv_values
from pyproj import CRS
from services.service_error import ServicesError


class FolderPathConfig:
    def __init__(self):
        self.PROJECT_FOLDER = path.abspath(
            path.join(path.dirname(__file__), "..")) + sep
        self.gis_config = self.setup_environment()
        self.load_app_vars()
        self.initialize_paths()

    def set_path(self, base_folder, foldername, seperate=False):
        return path.join(base_folder, foldername) + sep if seperate else path.join(base_folder, foldername)

    def str_to_bool(self, val):
        return self.gis_config.get(val) == "true"

    def setup_environment(self):
        """Sets up the environment variables using the setup_environment method."""
        gis_config = dotenv_values('.env.local')
        data_path = Path(__file__).resolve().parent.parent

        # Load EUNIS codes
        try:
            with open('./data/eunis_codes.json') as json_file:
                eunis_dict = json.load(json_file)
        except FileNotFoundError as err:
            eunis_dict = self.add_unique_eunis_codes(
                data_path / gis_config["input_all_habitats"])
            with open('./data/eunis_codes.json', 'w') as fp:
                json.dump(eunis_dict, fp)

        # Setup sensitivity matrix
        sens_mat = self.setup_sensitivity_matrix(
            maresa_file=gis_config["maresa"], jncc_file=gis_config["jncc"], data_path=data_path)

        # Add relevant gis_configurations
        gis_config["sensmat"] = sens_mat
        gis_config["eunis"] = eunis_dict
        gis_config["jose_crs_str"] = "+proj=aea +lat_1=43 +lat_2=62 +lat_0=30 +lon_0=-30 +x_0=0 +y_0=0 +datum=WGS84 +units=m +no_defs +ellps=WGS84 +towgs84=0,0,0"
        gis_config["wgs84_str"] = "+proj=longlat +ellps=WGS84 +datum=WGS84 +no_defs"
        gis_config["ogr2ogr_executable"] = "/usr/bin/ogr2ogr"

        return gis_config

    def load_app_vars(self):
        """Loads the server configuration from the file."""
        certfile = None if self.gis_config.get(
            'certfile') == "none" else self.gis_config.get('certfile')
        keyfile = None if self.gis_config.get(
            'keyfile') == "none" else self.gis_config.get('keyfile')

        self.MBAT = self.gis_config.get('mbat')
        self.CERTFILE = certfile
        self.KEYFILE = keyfile
        self.DISABLE_SECURITY = self.str_to_bool('disable_security')
        self.DISABLE_FILE_LOGGING = self.str_to_bool('disable_file_logging')
        self.ENABLE_RESET = self.str_to_bool('enable_reset')
        self.PERMITTED_DOMAINS = self.gis_config.get(
            'permitted_domains').split(",")
        self.PLANNING_GRID_UNITS_LIMIT = int(
            self.gis_config.get('planning_grid_unit_limit'))
        self.WDPA_VERSION = self.gis_config.get('wdpa_version')
        self.wgs84 = CRS.from_proj4(self.gis_config.get('wgs84_str'))
        self.jose_crs = CRS.from_proj4(self.gis_config.get('jose_crs_str'))

    def initialize_paths(self):
        """Initializes folder paths and executable paths."""
        self.USERS_FOLDER = self.set_path(
            self.PROJECT_FOLDER, "users", True)
        self.CLUMP_FOLDER = self.set_path(
            self.USERS_FOLDER, "_clumping", True)
        self.EXPORT_FOLDER = self.set_path(
            self.PROJECT_FOLDER, "exports", True)
        self.IMPORT_FOLDER = self.set_path(
            self.PROJECT_FOLDER, "imports", True)
        self.MARXAN_WEB_RESOURCES_FOLDER = self.set_path(
            self.PROJECT_FOLDER, "_marxan_web_resources", True)
        self.CASE_STUDIES_FOLDER = self.set_path(
            self.MARXAN_WEB_RESOURCES_FOLDER, "case_studies", True)
        self.EMPTY_PROJECT_TEMPLATE_FOLDER = self.set_path(
            self.MARXAN_WEB_RESOURCES_FOLDER, "empty_project", True)

    def load_maresa_data(self, maresa_file):
        """Load and clean the MarESA data."""
        eco_sys = pd.read_excel(maresa_file, sheet_name=2, engine='openpyxl')
        mar_esa = eco_sys.dropna(subset=['EUNIS_Code'])
        return mar_esa[["EUNIS_Code", "Pressure", "Sensitivity"]]

    def load_jncc_data(self, jncc_file):
        """Load and clean the JNCC data. Filter and select relevant columns, removing local pressures and NaNs"""
        jncc = pd.read_csv(jncc_file, encoding='unicode_escape')
        jncc_cleaned = jncc[~jncc.JNCC_Pressure.str.contains(
            'local', regex=False)]
        jncc_cleaned = jncc_cleaned[[
            "EUNIS_Code_Assessment", "MarESA_Pressure", "Sensitivity"]]
        jncc_cleaned = jncc_cleaned[~jncc_cleaned.MarESA_Pressure.isna()]
        jncc_cleaned = jncc_cleaned.drop_duplicates(
            subset=["EUNIS_Code_Assessment", "MarESA_Pressure"])
        jncc_cleaned.rename(columns={
                            'EUNIS_Code_Assessment': 'EUNIS_Code', 'MarESA_Pressure': 'Pressure'}, inplace=True)

        return jncc_cleaned.reset_index(drop=True)

    def process_sensitivity_matrix(self, mar_esa, jncc):
        """Combine MarESA and JNCC datasets and create the sensitivity matrix.
        Replace characters and pivot the table to create the matrix
        Convert sensitivity text to numerical values"""
        sens_mat = pd.concat([mar_esa, jncc])
        sens_mat['Pressure'] = sens_mat['Pressure'].map(self.replace_chars)
        sens_mat = sens_mat.pivot(
            index="EUNIS_Code", columns="Pressure", values="Sensitivity")
        sens_mat = sens_mat.map(self.sensitivity_text_to_int)
        return sens_mat

    def setup_sensitivity_matrix(self, maresa_file, jncc_file, data_path):
        """Main function to load and process the sensitivity matrix."""
        maresa_file_path = path.join(data_path, maresa_file)
        jncc_file_path = path.join(data_path, jncc_file)

        # Load the datasets
        mar_esa = self.load_maresa_data(maresa_file_path)
        jncc = self.load_jncc_data(jncc_file_path)

        # Process and return the sensitivity matrix
        return self.process_sensitivity_matrix(mar_esa, jncc)

    def sensitivity_text_to_int(self, text):
        """Convert sensitivity text to its corresponding integer value."""
        sensitivity_mapping = {
            "Low": 1,
            "Medium": 2,
            "High": 3
        }
        # Return mapped value or 0 if the text is not found
        return sensitivity_mapping.get(text, 0)

    def replace_chars(self, text):
        """Remove punctuation, replace multiple spaces with underscores, and convert to lowercase."""
        text = text.translate(str.maketrans("", "", string.punctuation))
        return re.sub(r'\s+', '_', text).lower()

    def add_unique_eunis_codes(self, shape_file):
        """
        Opens the specified shapefile, extracts unique EUNIS codes, and creates a dictionary 
        mapping EUNIS codes to integer IDs. Also adds a new field to the shapefile that stores 
        these integer codes.

        Args:
            shape_file (str): Path to the shapefile.

        Returns:
            dict: A dictionary mapping formatted EUNIS codes to unique integers.
        """
        all_habitats = gpd.read_file(shape_file)
        unique_eunis_codes = all_habitats.EUNIScombD.dropna().unique()
        unique_eunis_codes = unique_eunis_codes[unique_eunis_codes != 'Na']
        # Create a dictionary mapping formatted EUNIS codes to unique integers
        eunis_dict = {self.format_eunis(
            val): idx for idx, val in enumerate(unique_eunis_codes)}

        return eunis_dict

    def format_eunis(self, text):
        """
        Formats a EUNIS code by extracting the first part of the string, splitting on space or colon.

        Args:
            text (str): The EUNIS code string.

        Returns:
            str: The formatted EUNIS code (first part), or the original text if it is None or empty.
        """
        if text:
            return text.split()[0].split(':')[0]
        return text


folder_path_config = None


def get_folder_path_config():
    global folder_path_config
    if folder_path_config is None:
        folder_path_config = FolderPathConfig()
    return folder_path_config
