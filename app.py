import asyncio
import datetime
import glob
import json
import logging
import os
import platform
import shutil
import signal
import sys
import uuid
import webbrowser
from datetime import datetime, timedelta, timezone
from urllib.parse import urlparse

import colorama
import jwt
import numpy as np
import pandas as pd
import psutil
import psycopg2
import rasterio
from handlers.bioprotect_engage_handler import BioProtectEngageHandler
import tornado.httpserver
import tornado.options
from classes.db_config import DBConfig
from classes.folder_path_config import get_folder_path_config
from classes.postgis_class import get_pg
from colorama import Fore, Style
from functions.utils import (create_cost_from_impact, cumul_impact,
                             get_tif_list, pad_dict, replace_chars,
                             reproject_raster,
                             reproject_raster_to_all_habs)
from handlers.base_handler import BaseHandler
from handlers.feature_handler import FeatureHandler
from handlers.feature_websocket_handler import ImportFeaturesWSHandler
from handlers.planning_unit_handler import PlanningUnitHandler
from handlers.planning_unit_websocket_handler import PlanningGridWSHandler
from handlers.preprocess_feature_websocket_handler import PreprocessFeature
from handlers.project_handler import ProjectHandler
from handlers.user_handler import UserHandler
from handlers.prioritizr_handler import PrioritizrHandler
from handlers.prioritizr_websocket_handler import PrioritizrWSHandler
from handlers.activity_handler import UploadActivityHandler, RunCumulativeImpactHandler
from handlers.cost_raster_handler import (UploadRasterCostHandler,
                                          GetRasterBandInfoHandler,
                                          CostRasterLibraryHandler)
from handlers.cost_handler import (DeleteCostHandler,
                                   SetActiveCostProfileHandler,
                                   GetCostProfileActivitiesHandler,
                                   CreateCostsFromImpactHandler)
from handlers.notification_handler import NotificationHandler
from handlers.reference_layers_handler import ReferenceLayersHandler
from osgeo import ogr
from passlib.hash import bcrypt
from psycopg2 import sql
from rasterio.io import MemoryFile
from services.file_service import (delete_zipped_shapefile,
                                   get_output_file,
                                   read_file,  unzip_shapefile,
                                   update_file_parameters,
                                   write_to_file)
from services.martin_service import restart_martin
from services.project_service import write_csv
from services.run_command_service import run_command
from services.service_error import ServicesError, raise_error
from sqlalchemy import create_engine, exc
from tornado.escape import json_decode
from tornado.ioloop import IOLoop
from tornado.log import LogFormatter
from tornado.platform.asyncio import AnyThreadEventLoopPolicy
from tornado.process import Subprocess
from tornado.web import HTTPError, StaticFileHandler
import hashlib

####################################################################################################################################################################################################################################################################
# constant declarations
####################################################################################################################################################################################################################################################################

# SECURITY SETTINGS
PERMITTED_METHODS = ["getServerData", "RestartMartin",
                     "getProjectsWithGrids"]
"""REST services that do not need authentication/authorisation."""
ROLE_UNAUTHORISED_METHODS = {
    "ReadOnly": ["createProject", "upgradeProject", "createPlanningUnitGrid", "uploadFileToFolder", "uploadFile", "importFeatures", "updatePUFile",  "PreprocessFeature", "preprocessProtectedAreas", "runMarxan", "stopProcess", "clearRunLogs", "unzipShapefile", "getShapefileFieldnames", "importProject", 'updateCosts', 'deleteCost'],
    "User": ["clearRunLogs"],
    "Admin": []
}
"""Dict that controls access to REST services using role-based authentication. Add REST services that you want to lock down to specific roles - a class added to an array will make that method unavailable for that role"""
SERVER_VERSION = "v1.0.7"
GUEST_USERNAME = "guest"
NOT_AUTHENTICATED_ERROR = "Request could not be authenticated. No secure cookie found."
NO_REFERER_ERROR = "The request header does not specify a referer and this is required for CORS access."
# MAPBOX_USER = "blishten"
MAPBOX_USER = "craicerjack"
"""The default name for the Mapbox user account to store Vector tiles"""
# file prefixes
SOLUTION_FILE_PREFIX = "output_r"
MISSING_VALUES_FILE_PREFIX = "output_mv"
# export settings
EXPORT_F_SHP_FOLDER = "f_shps"
"""The name of the folder where feature shapefiles are exported to during a project export."""
EXPORT_PU_SHP_FOLDER = "pu_shps"
"""The name of the folder where planning grid shapefiles are exported to during a project export."""
EXPORT_F_METADATA = 'features.csv'
"""The name of the file that contains the feature metadata data during a project export."""
EXPORT_PU_METADATA = 'planning_grid.csv'
"""The name of the file that contains the planning grid metadata data during a project export."""
# gbif constants
GBIF_API_ROOT = "https://api.gbif.org/v1/"
"""The GBIF API root url"""
GBIF_CONCURRENCY = 10
"""How many concurrent download processes to do for GBIF."""
GBIF_PAGE_SIZE = 300
"""The page size for occurrence records for GBIF requests"""
GBIF_POINT_BUFFER_RADIUS = 1000
"""The radius in meters to buffer all lat/lng coordinates for GBIF occurrence data"""
GBIF_OCCURRENCE_LIMIT = 200000
"""From the GBIF docs here: https://www.gbif.org/developer/occurrence#search"""
UNIFORM_COST_NAME = "Equal area"
"""The name of the cost profile that is equal area."""
SHUTDOWN_EVENT = tornado.locks.Event()
"""A Tornado event to allow it to exit gracefully."""
PING_INTERVAL = 30000
"""Interval between regular pings to keep a connection alive when using websockets."""
SHOW_START_LOG = True
"""To disable the start logging from unit tests."""
LOGGING_LEVEL = logging.INFO
"""Tornado logging level that controls what is logged to the console - options are logging.INFO, logging.DEBUG, logging.WARNING, logging.ERROR, logging.CRITICAL. All SQL statements can be logged by setting this to logging.DEBUG."""

# pdoc3 dict to whitelist private members for the documentation
__pdoc__ = {}
privateMembers = ['get_geometry_type', 'clone_project', 'create_user', 'create_zipfile', 'delete_archive_files', '_deleteFeature',  'delete_records_in_text_file', 'delete_zipped_shapefile',  'finish_feature_import', '_getAllProjects', 'get_dict_value',   'get_key_value', 'get_keys', 'get_bp_log', 'get_notifications_data', 'get_output_file', 'get_projects_for_feature', 'get_projects_for_user', 'get_run_logs',
                  'get_safe_project_name', 'get_unique_feature_name', 'get_user_data', 'get_users_data', 'normalize_dataframe', 'pad_dict', '_preprocessProtectedAreas', 'puid_array_to_df', 'raise_error', 'read_file', '_reprocessProtectedAreas', 'run_command', '_setCORS', 'set_global_vars', 'unzip_shapefile', 'update_dataframe', 'update_file_parameters', '_uploadTileset', 'validate_args', 'write_csv', 'write_to_file', 'zip_folder']

for m in privateMembers:
    __pdoc__[m] = True


def log_server_info():
    """Logs server-related information."""
    log(f"Server {SERVER_VERSION} port {db_config.SERVER_PORT} ..", Fore.GREEN)
    log(pad_dict("Operating system:", platform.system()))
    log(pad_dict("Tornado version:", tornado.version))
    log(pad_dict("Permitted domains:", ",".join(
        project_paths.PERMITTED_DOMAINS)))
    log(pad_dict("SSL certificate file:",
        project_paths.CERTFILE if project_paths.CERTFILE != "None" else "None"))
    log(pad_dict("Private key file:",
        project_paths.KEYFILE if project_paths.KEYFILE != "None" else "None"))
    log(pad_dict("Database:", db_config.CONNECTION_STRING))


def logClientInfo():
    """Logs information about the Marxan client."""

    global FRONTEND_BUILD_FOLDER
    global MARXAN_CLIENT_VERSION

    parent_folder = os.path.abspath(os.path.join(
        project_paths.PROJECT_FOLDER, os.pardir)) + os.sep
    package_json_path = os.path.join(parent_folder, "frontend/package.json")
    FRONTEND_BUILD_FOLDER = os.path.join(parent_folder, "frontend/build")
    MARXAN_CLIENT_VERSION = "Not installed"

    # Check if package.json exists and retrieve the version if it does
    if os.path.exists(package_json_path):
        with open(package_json_path) as f:
            MARXAN_CLIENT_VERSION = json.load(f).get('version', 'Unknown')
    log(f"frontend {MARXAN_CLIENT_VERSION} installed", Fore.GREEN)


def log_other_info():
    # get the database version
    GDAL_ENV_VAR = os.environ.get('GDAL_DATA', "Not set")
    # Determine if SSL is enabled based on the presence of CERTFILE
    if project_paths.CERTFILE is not None:
        log(pad_dict("SSL certificate file:", project_paths.CERTFILE))
        protocol = "https://"
    else:
        log(pad_dict("SSL certificate file:", "None"))
        protocol = "http://"

    log(pad_dict("PostgreSQL:", DB_V_POSTGRES))
    log(pad_dict("PostGIS:", DB_V_POSTGIS))
    log(pad_dict("Planning grid limit:", project_paths.PLANNING_GRID_UNITS_LIMIT))
    log(pad_dict("Disable security:", project_paths.DISABLE_SECURITY))
    log(pad_dict("Disable file logging:", project_paths.DISABLE_FILE_LOGGING))
    log(pad_dict("Python executable:", sys.executable))
    log(pad_dict("ogr2ogr executable:", db_config.OGR2OGR_EXECUTABLE))
    log(pad_dict("GDAL_DATA path:", GDAL_ENV_VAR))
    log(pad_dict("Marxan executable:", db_config.MARXAN_EXECUTABLE))
    log(db_config.STOP_CMD, Fore.RED)


####################################################################################################################################################################################################################################################################
# generic functions that dont belong to a class so can be called by subclasses of tornado.web.RequestHandler and tornado.websocket.WebSocketHandler equally - underscores are used so they dont mask the equivalent url endpoints
####################################################################################################################################################################################################################################################################
project_paths = None
db_config = None


# Catch Ctrl+C or kill signals


async def set_global_vars():
    """set all of the global path variables"""
    global DB_V_POSTGRES
    global DB_V_POSTGIS
    global pg

    pg = await get_pg()

    results = await pg.execute("SELECT version(), PostGIS_Version();", return_format="Array")
    DB_V_POSTGRES = results[0]["version"]
    DB_V_POSTGIS = results[0]["postgis_version"]
    log_server_info()
    logClientInfo()
    # initialise colorama to be able to show log messages on windows in color
    colorama.init()
    # register numpy int64 with psycopg2
    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
    log_other_info()


def log(message, color=Fore.RESET):
    """Logs the string to the logging handlers using the passed colorama color

    Args:
        _str (string): The string to log
        _color (int): The color to use. The default is Fore.RESET.
    Returns:
        None
    """
    if SHOW_START_LOG:
        print(f"{color}{message}{Style.RESET_ALL}")
    if not project_paths.DISABLE_FILE_LOGGING:
        write_to_file(
            f"{project_paths.PROJECT_FOLDER}server.log", f"{message}\n", "a")


# get the information about which species have already been preprocessed
# gets the marxan log after a run
def get_bp_log(obj):
    """
    Retrieves the Marxan log from the log file after a run and sets it on the provided object.
    """
    log_file_path = os.path.join(obj.output_folder, "output_log.dat")
    print('log_file_path: ', log_file_path)
    obj.bpLog = read_file(
        log_file_path) if os.path.exists(log_file_path) else ""


def get_unique_feature_name(prefix):
    # mapbox tileset ids are limited to 32 characters
    return prefix + uuid.uuid4().hex[:(32 - len(prefix))]


async def finish_feature_import(feature_class_name, name, description, source, user):
    """
    Finalizes the creation of a feature by adding a spatial index, setting up a primary key,
    and inserting a record into the metadata_interest_features table.

    Args:
        feature_class_name (str): The feature class to finish creating.
        name (str): The name of the feature class used as an alias in the metadata_interest_features table.
        description (str): The description for the feature class.
        source (str): The source for the feature.
        user (str): The user who created the feature.

    Returns:
        int: The ID of the feature created.

    Raises:
        ServicesError: If the feature already exists or other errors occur.
    """
    # get the Mapbox tilesetId
    tileset_id = f"{MAPBOX_USER}.{feature_class_name}"
    index_name = f"idx_{uuid.uuid4().hex}"

    # create an index on the geometry column
    await pg.execute(
        sql.SQL("CREATE INDEX {} ON bioprotect.{} USING GIST (geometry);")
        .format(sql.Identifier(index_name), sql.Identifier(feature_class_name))
    )

    # Add a primary key to the table
    try:
        await pg.execute(
            sql.SQL("ALTER TABLE bioprotect.{} DROP COLUMN IF EXISTS id, DROP COLUMN IF EXISTS ogc_fid;"
                    ).format(sql.Identifier(feature_class_name))
        )
        await pg.execute(
            sql.SQL("ALTER TABLE bioprotect.{} ADD COLUMN id SERIAL PRIMARY KEY;")
            .format(sql.Identifier(feature_class_name))
        )
    except psycopg2.errors.InvalidTableDefinition as e:
        logging.warning(
            f"Primary key already exists for {feature_class_name}: {e}")

    # Insert metadata for the feature
    try:
        geometry_type = await pg.get_geometry_type(feature_class_name)

        if geometry_type != 'ST_Point':
            # Polygon layer: Calculate total area
            query = """
                INSERT INTO bioprotect.metadata_interest_features (
                    feature_class_name, alias, description, creation_date, _area, tilesetid, extent, source, created_by
                )
                SELECT %s, %s, %s, now(), sub._area, %s, sub.extent, %s, %s
                FROM (
                    SELECT ST_Area(ST_Transform(geom, 4326)) AS _area, box2d(geom) AS extent
                    FROM (
                        SELECT ST_Union(geometry) AS geom FROM bioprotect.{}
                    ) AS sub2
                ) AS sub
                RETURNING unique_id;
            """
        else:
            # Point layer: Calculate total amount
            query = """
                INSERT INTO bioprotect.metadata_interest_features (
                    feature_class_name, alias, description, creation_date, _area, tilesetid, extent, source, created_by
                )
                SELECT %s, %s, %s, now(), sub._area, %s, sub.extent, %s, %s
                FROM (
                    SELECT amount AS _area, box2d(combined) AS extent
                    FROM (
                        SELECT SUM(value) AS amount, ST_Collect(geometry) AS combined
                        FROM bioprotect.{}
                    ) AS sub2
                ) AS sub
                RETURNING unique_id;
            """

        feature_id = await pg.execute(
            sql.SQL(query).format(sql.Identifier(feature_class_name)),
            data=[feature_class_name, name,
                  description, tileset_id, source, user],
            return_format="Array"
        )

    except Exception as e:
        await pg.execute(sql.SQL("DROP TABLE IF EXISTS bioprotect.{};").format(sql.Identifier(feature_class_name)))

        if isinstance(e, psycopg2.errors.UniqueViolation) or "already exists" in str(e):
            raise ServicesError(
                f"A feature with the name '{name}' already exists. Please choose a different name.")

        logging.error(f"Unexpected error during metadata insert: {e}")
        raise ServicesError(
            "An unexpected error occurred while creating the feature.") from e

    return feature_id[0]


def _setCORS(obj):
    """Sets the CORS headers on the request to prevent CORS errors in the client.

    Args:
        obj (BaseHandler): The request handler instance.
    Returns:
        None
    Raises:
        ServicesError: If the request is not allowed to make cross-domain requests (based on the settings in the server.dat file).
    """
    # get the referer
    if "Referer" in list(obj.request.headers.keys()):
        referer = obj.request.headers.get("Referer")
        # get the origin
        parsed = urlparse(referer)
        origin = parsed.scheme + "://" + parsed.netloc
        # get the method
        method = obj.request.path.strip(
            "/").split("/")[-1] if obj.request.path else ""
        # check the origin is permitted either by being in the list of permitted domains or if the referer and host are on the same machine, i.e. not cross domain - OR if a permitted method is being called
        if (origin in project_paths.PERMITTED_DOMAINS) or (referer.find(obj.request.host_name) != -1) or (method in PERMITTED_METHODS):
            obj.set_header("Access-Control-Allow-Origin", origin)
            obj.set_header("Access-Control-Allow-Credentials", "true")
            obj.set_header("SameSite", "Lax")
        else:
            # , reason = "The origin '" + referer + "' does not have permission to access the service (CORS error)"
            raise HTTPError(403, "The origin '" + origin +
                            "' does not have permission to access the service (CORS error)")
    else:
        raise HTTPError(403, NO_REFERER_ERROR)


####################################################################################################################################################################################################################################################################
# RequestHandler subclasses
####################################################################################################################################################################################################################################################################


class methodNotFound(BaseHandler):
    """
    REST HTTP handler invoked when the REST service method does not match any defined handlers.
    """

    def prepare(self):
        """
        Overrides the `prepare` method to handle cases where a requested method is not found.
        """
        print("Method not found")
        error_message = "The method is not supported or the parameters are incorrect on this server."

        if 'Upgrade' in self.request.headers:
            # Handle unsupported WebSocket method
            raise tornado.web.HTTPError(501, reason=error_message)
        else:
            # Handle unsupported GET/POST method
            raise_error(self, error_message)


class AuthHandler(BaseHandler):

    async def post(self):
        try:
            # comment:
            body = json_decode(self.request.body)
            username = body.get("username")
            pwd = body.get("password")
            engage = body.get("engage", False)

            if not username or not pwd:
                self.set_status(400)
                self.write({"message": "Username and password required"})
                return

            # Query user from PostgreSQL
            query = """
                SELECT id, username, password_hash, role, last_project, show_popup, basemap, use_feature_colours, report_units, refresh_tokens
                FROM bioprotect.users WHERE username = %s
            """
            result = await pg.execute(query, [username], return_format="Dict")
            # notifications = get_notifications_data(self)

            if not result:
                self.set_status(401)
                self.write({"message": "Unauthorized."})
                self.send_response({
                    "status": 401,
                    "info": "Unauthorized. No user found with the provided username.",
                    "type": "error"
                })
                return

            user = result[0]
            print('user: ', user)

            # Verify password
            if not bcrypt.verify(pwd, user["password_hash"]):
                self.set_status(401)
                self.write({"message": "Unauthorized."})
                self.send_response({
                    "status": 401,
                    "info": "Unauthorized. No user found with the provided username.",
                    "type": "error"
                })
                return

            # Remove expired refresh tokens
            now = datetime.now()
            valid_refresh_tokens = []
            for token in user["refresh_tokens"] or []:
                try:
                    decoded_token = jwt.decode(token, self.proj_paths.gis_config.get(
                        "refresh_token"), algorithms=["HS256"])
                    if datetime.fromtimestamp(decoded_token["exp"]) > now:
                        valid_refresh_tokens.append(token)
                except (jwt.ExpiredSignatureError, jwt.InvalidTokenError):
                    continue

            # Generate tokens
            access_token = jwt.encode({
                "UserInfo": {"username": user["username"], "role": user["role"] or ""},
                "exp": now + timedelta(seconds=10),
            }, project_paths.gis_config.get("access_token"),  algorithm="HS256")

            refresh_token = jwt.encode({
                "username": user["username"],
                "exp": now + timedelta(seconds=15),
            }, project_paths.gis_config.get("refresh_token"), algorithm="HS256")

            valid_refresh_tokens.append(refresh_token)

            # Update refresh tokens in the database
            update_query = "UPDATE bioprotect.users SET refresh_tokens = %s WHERE id = %s"
            await pg.execute(update_query, [valid_refresh_tokens, user["id"]])

            # Set secure cookie for refresh token
            self.set_signed_cookie("user", username)
            self.set_signed_cookie("user_id", str(user['id']))
            self.set_signed_cookie("role", user['role'])
            self.set_cookie("jwt", refresh_token, httponly=True,
                            secure=True, samesite="None")

            # Remove sensitive fields before sending user data
            user.pop("password_hash")
            user.pop("refresh_tokens")

            if not engage:
                # Fetch user's projects with associated planning unit info
                project_query = """
                    SELECT p.id, p.name, p.description, p.date_created, up.role,
                           mpu.tilesetid AS pu_tilesetid,
                           mpu.alias AS pu_alias,
                           mpu.feature_class_name AS pu_feature_class_name
                    FROM bioprotect.projects p
                    JOIN bioprotect.user_projects up
                    ON up.project_id = p.id
                    LEFT JOIN bioprotect.metadata_planning_units mpu
                    ON mpu.unique_id = p.planning_unit_id
                    WHERE up.user_id = %s
                    ORDER BY LOWER(p.name)
                """

                project_result = await pg.execute(project_query, [user['id']], return_format="Dict")

                # Select the last accessed project if it exists
                last_project_id = user.get("last_project")
                selected_project = next(
                    (p for p in project_result if p["id"] == last_project_id),
                    project_result[0] if project_result else None
                )

                # Respond with access token and user data
                self.send_response({
                    "userId": user['id'],
                    "accessToken": access_token,
                    "userData": user,
                    "project": selected_project,
                    # Send user data along with authentication
                    # "dismissedNotification": notifications
                })
            else:
                self.send_response({
                    "userId": user['id'],
                    "accessToken": access_token,
                    "userData": user,
                })
        except ServicesError as e:
            raise_error(self, e.args[0])


class getServerData(BaseHandler):
    """REST HTTP handler. Gets the server configuration data from the server.dat file as an abject. The required arguments in the request.arguments parameter are:

    Args:
        None
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):
    """

    def get(self):
        try:
            # get the number of processors
            # get the virtual memory
            memory_gb = psutil.virtual_memory().total / (1024 ** 3)  # Convert bytes to GB
            memory = f"{memory_gb:.1f} Gb"

            # Update server data with system information
            self.server_data = {
                "RAM": memory,
                "PROCESSOR_COUNT": psutil.cpu_count(),
                "DATABASE_VERSION_POSTGIS": DB_V_POSTGRES,
                "DATABASE_VERSION_POSTGRESQL": DB_V_POSTGIS,
                "SYSTEM": platform.system(),
                "NODE": platform.node(),
                "RELEASE": platform.release(),
                "VERSION": platform.version(),
                "MACHINE": platform.machine(),
                "PROCESSOR": platform.processor(),
                "SERVER_VERSION": SERVER_VERSION,
                "MARXAN_CLIENT_VERSION": MARXAN_CLIENT_VERSION,
                "SERVER_NAME": db_config.SERVER_NAME,
                "SERVER_DESCRIPTION": db_config.SERVER_DESCRIPTION,
                "SERVER_PORT": db_config.SERVER_PORT,
                "ENABLE_RESET": project_paths.ENABLE_RESET,
                "PERMITTED_DOMAINS": project_paths.PERMITTED_DOMAINS,
                "CERTFILE": project_paths.CERTFILE,
                "KEYFILE": project_paths.KEYFILE,
                "PLANNING_GRID_UNITS_LIMIT": project_paths.PLANNING_GRID_UNITS_LIMIT,
                "DISABLE_SECURITY": project_paths.DISABLE_SECURITY,
                "DISABLE_FILE_LOGGING": project_paths.DISABLE_FILE_LOGGING,
                "WDPA_VERSION": project_paths.WDPA_VERSION,
                "DISK_FREE_SPACE": memory,
            }

            # get any shutdown timeouts if they have been set
            shutdownTime = read_file(project_paths.PROJECT_FOLDER + "shutdown.dat") if (
                os.path.exists(project_paths.PROJECT_FOLDER + "shutdown.dat")) else None
            if shutdownTime:
                self.server_data.update({'SHUTDOWNTIME': shutdownTime})
            # set the response
            self.send_response(
                {'info': 'Server data loaded', 'serverData': self.server_data})
        except ServicesError as e:
            raise_error(self, e.args[0])


class uploadFileToFolder(BaseHandler):
    """REST HTTP handler. Uploads a file to a specific folder within the Marxan root folder. The required arguments in the request.arguments parameter are:

    Args:
        files(bytes): The file data to upload.
        filename (string): The name of the file to be uploaded.
        destFolder (string): The folder path on the server to upload the file to relative to the project_paths.PROJECT_FOLDER, e.g. export.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "file": The name of the file that was uploaded
        }
    """

    def post(self):
        try:
            # validate the input arguments
            self.validate_args(self.request.arguments, ['filename', 'destFolder'])
            filename = self.get_argument('filename')
            dest_folder = self.get_argument('destFolder')
            print('====================== dest_folder: ', dest_folder)
            # write the file to the server
            file_path = project_paths.PROJECT_FOLDER + dest_folder + os.sep + filename
            print("============================== Writing file to:", file_path)
            write_to_file(
                file_path, self.request.files["value"][0]["body"], 'wb')
            self.send_response({
                'info': f"File '{filename}' uploaded",
                'file': filename,
                'file_path': file_path
            })
        except ServicesError as e:
            raise_error(self, e.args[0])


class uploadFile(BaseHandler):
    """REST HTTP handler. Uploads a file to the Marxan users project folder. The required arguments in the request.arguments parameter are:

    Args:
        user (string): The name of the user.
        project (string): The name of the project.
        files(bytes): The file data to upload.
        filename (string): The name of the file to be uploaded.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def post(self):
        try:
            # validate the input arguments
            self.validate_args(self.request.arguments, [
                'user', 'project', 'filename'])
            # write the file to the server
            write_to_file(self.project_folder + self.get_argument('filename'),
                          self.request.files['value'][0].body, 'wb')
            # set the response
            self.send_response({'info': "File '" + self.get_argument('filename') +
                                "' uploaded", 'file': self.get_argument('filename')})
        except ServicesError as e:
            raise_error(self, e.args[0])


class unzipShapefile(BaseHandler):
    """REST HTTP handler. Unzips an already uploaded shapefile and returns the rootname. The required arguments in the request.arguments parameter are:

    Args:
        filename (string): The name of the zip file that will be unzipped in the project_paths.IMPORT_FOLDER.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "rootfilename": The name of the shapefile unzipped (minus the .shp extension)
        }
    """

    async def get(self):
        try:
            # validate the input arguments
            self.validate_args(self.request.arguments, ['filename'])
            filename = self.get_argument('filename')
            filepath = project_paths.IMPORT_FOLDER
            print('===================== filepath: ', filepath)
            # write the file to the server
            rootfilename = await IOLoop.current().run_in_executor(
                None, unzip_shapefile, filepath, filename)
            # set the response
            self.send_response({
                'info': f"File '{filename}' unzipped",
                'rootfilename': rootfilename
            })
        except ServicesError as e:
            raise_error(self, e.args[0])


class getShapefileFieldnames(BaseHandler):
    """REST HTTP handler. Gets a field list from a shapefile. The required arguments in the request.arguments parameter are:

    Args:
        filename (string): The name of the shapefile (minus the *.shp extension).
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message,
            "fieldnames": string[]: A list of the field names
        }
    """

    def get(self):
        ogr.UseExceptions()
        try:
            self.validate_args(self.request.arguments, ['filename'])

            # load the shapefile
            shapefile = project_paths.IMPORT_FOLDER + \
                self.get_argument('filename')
            data_source = ogr.Open(shapefile)
            if not data_source:
                raise ServicesError(f"Shapefile '{shapefile}' not found")

            layer = data_source.GetLayer(0)
            layer_definition = layer.GetLayerDefn()

            fields = [layer_definition.GetFieldDefn(x).GetName(
            ) for x in range(layer_definition.GetFieldCount())]

            values = []
            layer.ResetReading()
            for i, value in enumerate(layer):
                if i >= 30:
                    break
                row = {field: value.GetField(field) for field in fields}
                values.append(row)
            print('******************* values: ', values)

            # set the response
            self.send_response(
                {'info': "Field list returned", 'fieldnames': fields, 'values': values})
        except ServicesError as e:
            raise_error(self, e.args[0])


class deleteShapefile(BaseHandler):
    """REST HTTP handler. Deletes a zipped shapefile and its unzipped files (if present). The required arguments in the request.arguments parameter are:

    Args:
        zipfile (string): The name of the zipped shapfile.
        shapefile (string): The root name of the shapefile - this will be used to match the unzipped files in the folder and delete them.
    Returns:
        A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Informational message
        }
    """

    def get(self):
        try:
            # validate the input arguments
            self.validate_args(self.request.arguments, [
                'zipfile', 'shapefile'])
            delete_zipped_shapefile(project_paths.IMPORT_FOLDER, self.get_argument(
                'zipfile'), self.get_argument('shapefile')[:-4])
            # set the response
            self.send_response({'info': "Shapefile deleted"})
        except ServicesError as e:
            raise_error(self, e.args[0])


class RestartMartin(BaseHandler):
    async def get(self):
        try:
            if not project_paths.ENABLE_RESET:
                raise ValueError(
                    "Restart endpoint is disabled by config (ENABLE_RESET=false).")
            self.send_response(restart_martin())
        except Exception as e:
            raise_error(self, str(e))

    async def post(self):
        try:
            if not project_paths.ENABLE_RESET:
                raise ValueError(
                    "Restart endpoint is disabled by config (ENABLE_RESET=false).")
            self.send_response(restart_martin())
        except Exception as e:
            raise_error(self, str(e))


#  ***********************************
#  *******************************************
#  ******************************************************
#  ************************************************************************
#  *********************************************************************************
#  ***************************************************************************************************
# * tornado functions
#  ***************************************************************************************************
#  *********************************************************************************
#  ******************************************************
#  ***********************************


def getPressuresActivitiesDatabase(padfile_path):
    db_url = (
        f"postgresql://{db_config.DATABASE_USER}:"
        f"{db_config.DATABASE_PASSWORD}@"
        f"{db_config.DATABASE_HOST}/"
        f"{db_config.DATABASE_NAME}"
    )
    print('db_url: ', db_url)

    engine = create_engine(db_url)
    print('engine: ', engine)
    try:
        pad = pd.read_sql('select * from bioprotect.pad', con=engine)
    except exc.ProgrammingError as err:
        print(err)
    finally:
        pad = pd.read_csv(padfile_path)
        pad.columns = pad.columns.str.lower()
        pad["rppscore"] = np.where(
            pad['rpptitle'] == 'low', 0.3, 1)
        pad.to_sql('pad', con=engine, schema='bioprotect', if_exists='replace')
    return pad


def make_stable_id(name: str, digits=6):
    h = hashlib.sha1(name.encode()).hexdigest()
    return int(h[:digits], 16)


class GetActivitiesHandler(BaseHandler):

    async def get(self):
        pad = getPressuresActivitiesDatabase(db_config.db_config.get('pad'))
        try:
            activities = []
            activitytitles = pad.activitytitle.unique()
            for idx, act in enumerate(activitytitles):
                act_id = make_stable_id(act)
                cat = pad[pad.activitytitle == act].categorytitle.unique()[0]
                activities.append({
                    "id": act_id,
                    "category": cat,
                    "activity": act
                })
            self.send_response({"data": json.dumps(activities)})
        except Exception as e:
            print(self, e.args[0])


class GetUploadedActivitiesHandler(BaseHandler):
    """
        REST HTTP handler. Gets all species information from the PostGIS database. The required arguments in the request.arguments parameter are:

        Args:
            None
        Returns:
            A dict with the following structure (if the class raises an exception, the error message is included in an 'error' key/value pair):

            {
            "info": Informational message,
            "data": dict[]: A list of the features. Each dict contains the keys: id,feature_class_name,alias,description,area,extent,creation_date,tilesetid,source,created_by
            }
    """

    async def get(self):
        print('Get all uploaded activities handerler.....')
        try:
            # get all the species data
            query = """
                SELECT id, filename, activity, description, to_char(creation_date, 'DD/MM/YY HH24:MI:SS')::text
                AS creation_date, source, created_by
                FROM bioprotect.metadata_activities
                ORDER BY lower(activity);
            """
            self.allUploadedActivities = await pg.execute(query, return_format="DataFrame")

            # set the response
            self.send_response({"info": "All activity data received",
                                "data": self.allUploadedActivities.to_dict(orient="records")})
        except Exception as e:
            print(self, e.args[0])


# tornado functions
####################################################################################################################################################################################################################################################################


class Application(tornado.web.Application):
    """Tornado Application class which defines all of the request handlers."""

    def __init__(self):
        if not hasattr(db_config, 'COOKIE_SECRET') or not db_config.COOKIE_SECRET:
            raise ValueError("db_config.COOKIE_SECRET is not set.")

        print("PG at Application init:", pg)

        settings = self._define_settings()
        handlers = self._define_handlers()
        super(Application, self).__init__(handlers, **settings)

    def _define_settings(self):
        """Define settings for the Tornado application."""
        return {
            'cookie_secret': db_config.COOKIE_SECRET,
            'static_path': project_paths.EXPORT_FOLDER,
            'static_url_prefix': '/resources/',
            # Ping every WebSocket so nginx (60s idle proxy_read_timeout) does
            # not drop quiet ones, e.g. /server/notifications.
            'websocket_ping_interval': 30
        }

    def _define_handlers(self):
        """Define all request handlers for the application."""

        return [
            ("/server/auth", AuthHandler),
            ("/server/projects", ProjectHandler, dict(pg=pg)),
            ("/server/users", UserHandler, dict(pg=pg, project_paths=project_paths)),
            ("/server/features", FeatureHandler, dict(pg=pg,
                                                      finish_feature_import=finish_feature_import)),
            ("/server/planning-units", PlanningUnitHandler, dict(pg=pg)),
            ("/server/prioritizr", PrioritizrHandler, dict(pg=pg)),
            ("/server/prioritizr-ws", PrioritizrWSHandler, dict(pg=pg,
                                                                r_script_path="./services/run_prioritzr_v2.R")),
            ("/server/engage", BioProtectEngageHandler, dict(pg=pg)),
            ("/server/notifications", NotificationHandler),

            ("/server/deleteCost", DeleteCostHandler, dict(pg=pg)),
            ("/server/setActiveCostProfile",
             SetActiveCostProfileHandler, dict(pg=pg)),
            ("/server/getCostProfileActivities",
             GetCostProfileActivitiesHandler, dict(pg=pg)),
            ("/server/createCostsFromImpact",
             CreateCostsFromImpactHandler, dict(pg=pg)),

            ("/server/importFeatures", ImportFeaturesWSHandler,
             dict(pg=pg,
                  finish_feature_import=finish_feature_import,
                  get_unique_feature_name=get_unique_feature_name)),
            ("/server/deleteShapefile", deleteShapefile),

            ("/server/createPlanningUnitGrid",
             PlanningGridWSHandler, dict(pg=pg)),  # websocket

            ("/server/getServerData", getServerData),
            ("/server/reference-layers", ReferenceLayersHandler, dict(pg=pg)),
            ("/server/getActivities", GetActivitiesHandler),

            ("/server/uploadFileToFolder", uploadFileToFolder),
            ("/server/unzipShapefile", unzipShapefile),
            ("/server/getShapefileFieldnames", getShapefileFieldnames),

            ("/server/preprocessFeature", PreprocessFeature, dict(pg=pg)),

            ("/server/restart-martin", RestartMartin),

            ("/server/getUploadedActivities", GetUploadedActivitiesHandler),
            ("/server/uploadActivity", UploadActivityHandler, dict(pg=pg)),
            ("/server/runCumulativeImpact", RunCumulativeImpactHandler, dict(pg=pg)),
            ("/server/uploadRasterCost", UploadRasterCostHandler, dict(pg=pg)),
            ("/server/getRasterBandInfo", GetRasterBandInfoHandler, dict(pg=pg)),
            ("/server/costRasters", CostRasterLibraryHandler, dict(pg=pg)),
            ("/server/uploadFile", uploadFile),


            ("/server/exports/(.*)", StaticFileHandler,
             {"path": project_paths.EXPORT_FOLDER}),
            # default handler if the REST services is cannot be found on this server - maybe a newer client is requesting a method on an old server
            ("/server/(.*)", methodNotFound),
            # assuming the client is installed in the same folder as the server all files will go to the client build folder
            (r"/(.*)", StaticFileHandler, {"path": FRONTEND_BUILD_FOLDER})
        ]


async def initialiseApp():
    """Initialises the application with all of the global variables"""

    global project_paths, db_config

    project_paths = get_folder_path_config()
    db_config = DBConfig()
    await set_global_vars()

    # LOGGING SECTION
    # turn on logging. Get parent logger. Set the logging level. Set format for streaming logger
    tornado.options.parse_command_line()
    root_logger = logging.getLogger()
    root_logger.setLevel(LOGGING_LEVEL)

    root_streamhandler = root_logger.handlers[0]
    f1 = '%(color)s[%(levelname)1.1s %(asctime)s.%(msecs)03d]%(end_color)s '
    f2 = '%(message)s'
    root_streamhandler.setFormatter(LogFormatter(fmt=f1 + f2,
                                                 datefmt='%d-%m-%y %H:%M:%S',
                                                 color=True))
    # add a file logger
    if not project_paths.DISABLE_FILE_LOGGING:
        file_log_handler = logging.FileHandler(
            os.path.join(project_paths.PROJECT_FOLDER, 'server.log'))
        file_log_handler.setFormatter(LogFormatter(
            fmt=f1 + f2, datefmt='%d-%m-%y %H:%M:%S', color=False))
        root_logger.addHandler(file_log_handler)

    app = Application()
    # nginx allows uploads up to client_max_body_size 500M, but Tornado's own
    # default is 100MB - without these limits anything in between passes nginx
    # and is then rejected here. Keep MAX_UPLOAD_BYTES in step with nginx.conf.
    MAX_UPLOAD_BYTES = 500 * 1024 * 1024
    # if there is an https certificate then use the certificate information from the server.dat file to return data securely
    if project_paths.CERTFILE is None:
        http_server = tornado.httpserver.HTTPServer(
            app,
            max_body_size=MAX_UPLOAD_BYTES,
            max_buffer_size=MAX_UPLOAD_BYTES)
    else:
        http_server = tornado.httpserver.HTTPServer(
            app,
            max_body_size=MAX_UPLOAD_BYTES,
            max_buffer_size=MAX_UPLOAD_BYTES,
            ssl_options={
                "certfile": project_paths.CERTFILE,
                "keyfile": project_paths.KEYFILE
            })
    http_server.listen(int(db_config.SERVER_PORT), address="0.0.0.0")

    protocol = "https://" if project_paths.CERTFILE != None else "http://"

    if db_config.SERVER_PORT != 80:
        navigateTo = f"{protocol}<host>:{db_config.SERVER_PORT}/index.html"
    else:
        navigateTo = f"{protocol}<host>/index.html"

    # open the web browser if the call includes a url, e.g. python server.py http://localhost/index.html
    if len(sys.argv) > 1:
        if MARXAN_CLIENT_VERSION == "Not installed":
            log("Ignoring <url> parameter - the client is not installed", Fore.GREEN)
        else:
            url = sys.argv[1]  # normally "http://localhost/index.html"
            log(f"Opening Marxan Web at {url} ..\n  {Fore.GREEN}")
            webbrowser.open(url, new=1, autoraise=True)

    elif MARXAN_CLIENT_VERSION != "Not installed":
        log(f"Goto to {navigateTo} to open Marxan Web {Fore.GREEN}")
        log(
            f"Or run 'python server.py {navigateTo} to open a browser\n {Fore.GREEN}")
    logging.warning("server started")
    # otherwise subprocesses fail on windows
    if platform.system() == "Windows":
        asyncio.set_event_loop_policy(AnyThreadEventLoopPolicy())

    await SHUTDOWN_EVENT.wait()
    log("Closing Postgres connections..")
    # close the database connection
    pg.pool.close()
    await pg.pool.wait_closed()


if __name__ == "__main__":
    try:
        tornado.ioloop.IOLoop.current().run_sync(initialiseApp)

    except KeyboardInterrupt:
        shutdown_dat = project_paths.PROJECT_FOLDER + "shutdown.dat"
        if (os.path.exists(shutdown_dat)):
            logging.warning("Deleting the shutdown file")
            os.remove(shutdown_dat)
        logging.warning("KeyboardInterrupt received, shutting down.")
        SHUTDOWN_EVENT.set()

    except Exception as e:
        if e.args and e.args[0] == 98:
            log(f"The port {db_config.SERVER_PORT} is already in use")
        else:
            log(f"Unhandled exception: {e}")
        SHUTDOWN_EVENT.set()
    finally:
        logging.warning("server stopped")
        SHUTDOWN_EVENT.set()
