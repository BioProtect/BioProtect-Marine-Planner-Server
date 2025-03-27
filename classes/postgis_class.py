import logging
import os
import uuid
from subprocess import CalledProcessError

import asyncpg
import pandas as pd
from classes.db_config import get_db_config
from services.file_service import check_zipped_shapefile
from services.run_command_service import run_command
from services.service_error import ServicesError


class PostGIS:
    """Utility class for interacting with PostGIS and managing data."""

    def __init__(self):
        self.pool = None
        self.config = get_db_config()

    async def initialise(self):
        """Initializes the connection pool to PostGIS."""
        try:
            print('self.config.CONNECTION_STRING,: ',
                  self.config.CONNECTION_STRING,)

            self.pool = await asyncpg.create_pool(
                dsn=self.config.CONNECTION_STRING,
                min_size=50,
                max_size=250,
                timeout=None
            )
        except Exception as e:
            logging.error(f"Error initializing PostGIS pool: {e}")
            raise ServicesError(
                "Failed to initialize the connection pool.") from e

    async def close_pool(self):
        """Closes the connection pool."""
        if self.pool:
            await self.pool.close()

    async def execute(self, sql_query, data=None, return_format=None, filename=None, socket_handler=None):
        """Executes a query and optionally returns the records or writes them to a file.

        Args:
            sql_query (str): The SQL query to execute.
            data (list): Optional. Parameters for the SQL query.
            return_format (str): Optional. Format of the return data: 'Array', 'DataFrame', 'Dict', or 'File'.
            filename (str): Optional. Name of the file if exporting results.
            socket_handler: Optional. Used for tracking query progress via WebSocket.

        Returns:
            Any: The result based on return_format or None.

        Raises:
            ServicesError: If any database operation fails.
        """
        async with self.pool.acquire() as conn:
            try:
                # Track the process ID if using a WebSocket handler
                if socket_handler:
                    pid = await conn.fetchval("SELECT pg_backend_pid()")
                    socket_handler.pid = f'q{pid}'
                    socket_handler.send_response(
                        {'status': 'pid', 'pid': socket_handler.pid})

                 # Debug: Log SQL query and parameters to verify format
                logging.debug(f"Executing SQL Query: {sql_query}")
                print('(f"Executing SQL Query: {sql_query}"): ',
                      (f"Executing SQL Query: {sql_query}"))
                logging.debug(f"With Parameters: {data}")
                print('(f"With Parameters: {data}"): ',
                      (f"With Parameters: {data}"))

                records = await conn.fetch(sql_query, *data) if data else await conn.fetch(sql_query)

                if return_format is None:
                    # Execute only if no data retrieval is required
                    return None

                else:
                    # Fetch all records if data is required
                    return self.format_results(records, return_format, filename)
            except Exception as e:
                logging.error(f"Error executing SQL: {e}")
                raise ServicesError(f"Database query failed: {str(e)}") from e
                # query = cur.mogrify(sql_query, data) if data else sql_query
                # await cur.execute(query)
                # if return_format is None:
                #     return None

                # records = await cur.fetchall()
                # return self.format_results(records, cur.description, return_format, filename)

    def format_results(self, records, return_format, filename):
        """Formats the results based on the specified return_format."""
        # Convert `Record` objects to lists of dictionaries
        data = [dict(record) for record in records]

        if (return_format in ["Array", "Dict"]):
            return data
        elif return_format == "DataFrame":
            return pd.DataFrame(data)
        elif return_format == "File" and filename:
            df = pd.DataFrame(data)
            df.to_csv(filename, index=False)
            return None
        return None

    async def drop_existing_table(self, feature_class_name):
        """Drops the feature class table if it already exists."""
        await self.execute(f"DROP TABLE IF EXISTS marxan.{feature_class_name};")

    def build_ogr2ogr_command(self, folder, filename, feature_class_name, s_epsg_code, t_epsg_code, source_feature_class=''):
        """Builds the ogr2ogr command string for importing data."""
        return (
            f'"{self.config.OGR2OGR_EXECUTABLE}" -f "PostgreSQL" PG:"host={
                self.config.DATABASE_HOST} user={self.config.DATABASE_USER} '
            f'dbname={self.config.DATABASE_NAME} password={
                self.config.DATABASE_PASSWORD}" "{os.path.join(folder, filename)}" '
            f'-nlt GEOMETRY -lco SCHEMA=marxan -lco GEOMETRY_NAME=geometry {
                source_feature_class} -nln {feature_class_name} '
            f'-s_srs {s_epsg_code} -t_srs {t_epsg_code} -lco precision=NO'
        )

    async def export_to_shapefile(self, export_folder, feature_class_name, t_epsg_code="EPSG:4326"):
        """Exports a feature class from postgis to a shapefile using ogr2ogr.

        Args:
            export_folder (string): The full path to where the shapefile will be exported.
            feature_class_name (string): The name of the feature class in PostGIS to export.
            t_epsg_code (string): Optional. The target EPSG code. Default value is 'EPSG:4326' (WGS84).
        Returns:
            int: Returns 0 if successful otherwise 1.
        Raises:
            ServicesError: If the ogr2ogr import fails.
        """
        # get the command to execute
        cmd = (
            f'"{self.config.OGR2OGR_EXECUTABLE}" -f "ESRI Shapefile" "{
                export_folder}" PG:"host={self.config.DATABASE_HOST} '
            f'user={self.config.DATABASE_USER} dbname={
                self.config.DATABASE_NAME} password={self.config.DATABASE_PASSWORD} '
            f'ACTIVE_SCHEMA=marxan" -sql "SELECT * FROM {
                feature_class_name};" -nln {feature_class_name} -t_srs {t_epsg_code}'
        )
        # run the command
        try:
            result = await run_command(cmd)
            if result != 0:
                raise ServicesError(f"Export failed with return code {result}")
            return result
        except CalledProcessError as e:
            raise ServicesError(f"Error exporting shapefile: {
                                e.output.decode('utf-8')}")

    async def import_file(self, folder, filename, feature_class_name, s_epsg_code, t_epsg_code, split_at_dateline=True, source_feature_class=''):
        """Imports a file or feature class into PostGIS using ogr2ogr.

        Args:
            folder (str): Path to the file's folder.
            filename (str): Name of the file to import.
            feature_class_name (str): Name of the destination feature class.
            s_epsg_code (str): Source EPSG code.
            t_epsg_code (str): Target EPSG code.
            split_at_dateline (bool): Whether to split features at the dateline.
            source_feature_class (str): Optional. Source feature class within the file.

        Raises:
            ServicesError: If the import fails.
        """
        await self.drop_existing_table(feature_class_name)
        cmd = self.build_ogr2ogr_command(
            folder, filename, feature_class_name, s_epsg_code, t_epsg_code, source_feature_class)
        logging.debug(f"Running ogr2ogr command: {cmd}")

        result = await run_command(cmd)
        if result != 0:
            raise ServicesError(f"Import failed with return code {result}")

        if split_at_dateline:
            await self.execute(f"UPDATE marxan.{feature_class_name} SET geometry = ST_SplitAtDateLine(geometry);")

    async def import_shapefile(self, folder, shapefile, feature_class_name, s_epsg_code="EPSG:4326", t_epsg_code="EPSG:4326", splitAtDateline=True):
        """Imports a shapefile into PostGIS using ogr2ogr.

        Args:
            folder (string): The full path to where the shapefile is located.
            shapefile (string): The name of the shapefile to import.
            feature_class_name (string): The name of the destination feature class which will be created.
            s_epsg_code (string): Optional. The source EPSG code. Default value is 'EPSG:4326' (WGS84).
            t_epsg_code (string): Optional. The target EPSG code. Default value is 'EPSG:4326' (WGS84).
            splitAtDateline (bool): Optional. Set to True to split any features at the dateline. Default value is True.
        Returns:
            None
        Raises:
            ServicesError: If the ogr2ogr import fails.
        """
        # check that all the required files are present for the shapefile
        check_zipped_shapefile(folder + shapefile)
        await self.import_file(folder, shapefile, feature_class_name, s_epsg_code, t_epsg_code, splitAtDateline)

    async def import_gml(self, folder, gmlfilename, feature_class_name, s_epsg_code="EPSG:4326", t_epsg_code="EPSG:4326", splitAtDateline=True):
        """Imports a gml file into PostGIS using ogr2ogr."""
        await self.import_file(folder, gmlfilename, feature_class_name, s_epsg_code, t_epsg_code, splitAtDateline)

    async def import_file_GDBFeatureClass(self, folder, fileGDB, sourceFeatureClass, destFeatureClass, s_epsg_code="EPSG:4326", t_epsg_code="EPSG:4326", splitAtDateline=True):
        """Imports a feature class in a file geodatabase into PostGIS using ogr2ogr."""
        # import the file
        await self.import_file(folder, fileGDB, destFeatureClass, s_epsg_code, t_epsg_code, splitAtDateline, sourceFeatureClass)

    async def is_valid(self, feature_class_name):
        """Validates geometries in the feature class."""
        result = await self.execute(f"SELECT DISTINCT ST_IsValid(geometry) FROM marxan.{feature_class_name} LIMIT 1;", return_format="Array")
        if not result[0][0]:
            await self.drop_existing_table(feature_class_name)
            raise ServicesError("The input shapefile has invalid geometries.")

    async def create_primary_key(self, feature_class_name, column):
        """Creates a primary key on the specified column."""
        key_name = f"idx_{uuid.uuid4().hex}"
        await self.execute(f"ALTER TABLE marxan.{feature_class_name} ADD CONSTRAINT {key_name} PRIMARY KEY ({column});")

    async def get_geometry_type(self, feature_class_name):
        """Gets the geometry type of the feature class."""
        result = await self.execute(f"SELECT ST_GeometryType(geometry) FROM marxan.{feature_class_name} LIMIT 1;", return_format="Array")
        return result[0][0]


pg = None


async def get_pg():
    global pg
    if pg is None:
        pg = PostGIS()
        await pg.initialise()
    return pg
