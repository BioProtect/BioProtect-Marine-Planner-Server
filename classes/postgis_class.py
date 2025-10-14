import logging
import os
import uuid
from subprocess import CalledProcessError

import aiopg
from psycopg2 import sql, OperationalError, IntegrityError, errors

import pandas as pd
from classes.db_config import get_db_config
from services.file_service import check_zipped_shapefile
from services.run_command_service import run_command
from services.service_error import ServicesError


class PostGIS:
    def __init__(self):
        self.pool = None
        self.config = get_db_config()

    async def initialise(self):
        try:
            self.pool = await aiopg.create_pool(
                dsn=self.config.CONNECTION_STRING,
                timeout=None,
                minsize=50,
                maxsize=250
            )
        except Exception as e:
            logging.error(f"Error initializing PostGIS pool: {e}")
            raise ServicesError(
                "Failed to initialize the connection pool.") from e

    async def close_pool(self):
        if self.pool:
            self.pool.close()
            await self.pool.wait_closed()

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
            async with conn.cursor() as cur:
                try:
                    logging.debug(f"Executing SQL Query: {sql_query}")
                    if socket_handler:
                        pid = conn.get_backend_pid()
                        socket_handler.pid = f'q{pid}'
                        socket_handler.send_response(
                            {'status': 'pid', 'pid': socket_handler.pid})

                    if data is not None:
                        await cur.execute(sql_query, data)
                    else:
                        await cur.execute(sql_query)

                    if return_format is None:
                        return None

                    records = await cur.fetchall()
                    columns = [desc[0] for desc in cur.description]

                    if return_format == "Array":
                        return [dict(zip(columns, row)) for row in records]

                    df = pd.DataFrame.from_records(records, columns=columns)
                    if return_format == "DataFrame":
                        return df
                    elif return_format == "Dict":
                        if len(columns) == 2 and columns == ['key', 'value']:
                            return {row[0]: row[1] for row in records}
                        return df.to_dict(orient="records")
                    elif return_format == "File" and filename:
                        df.to_csv(filename, index=False)
                    else:
                        return None
                except errors.UniqueViolation:
                    raise ServicesError("That item already exists")
                except errors.InternalError as e:
                    raise ServicesError("Query stopped: " + str(e))
                except Exception as e:
                    logging.error(f"Error executing SQL: {e}")
                    raise ServicesError(
                        f"Database query failed: {str(e)}") from e

    async def drop_existing_table(self, feature_class_name):
        await self.execute(f"DROP TABLE IF EXISTS bioprotect.{feature_class_name};")

    def build_ogr2ogr_command(self, folder, filename, feature_class_name, s_epsg_code, t_epsg_code, source_feature_class='', where_clause=None):
        where_part = f' -where "{where_clause}"' if where_clause else ''
        return (
            f'"{self.config.OGR2OGR_EXECUTABLE}" -f "PostgreSQL" PG:"host={self.config.DATABASE_HOST} '
            f'user={self.config.DATABASE_USER} dbname={self.config.DATABASE_NAME} password={self.config.DATABASE_PASSWORD}" '
            f'"{os.path.join(folder, filename)}" -nlt GEOMETRY -lco SCHEMA=bioprotect '
            f'-lco GEOMETRY_NAME=geometry {source_feature_class} -nln {feature_class_name} '
            f'-s_srs {s_epsg_code} -t_srs {t_epsg_code} -lco precision=NO{where_part}'
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
        cmd = (
            f'"{self.config.OGR2OGR_EXECUTABLE}" -f "ESRI Shapefile" "{export_folder}" PG:"host={self.config.DATABASE_HOST} '
            f'user={self.config.DATABASE_USER} dbname={self.config.DATABASE_NAME} password={self.config.DATABASE_PASSWORD} '
            f'ACTIVE_SCHEMA=bioprotect" -sql "SELECT * FROM {feature_class_name};" -nln {feature_class_name} -t_srs {t_epsg_code}'
        )
        try:
            result = await run_command(cmd)
            if result != 0:
                raise ServicesError(f"Export failed with return code {result}")
            return result
        except CalledProcessError as e:
            raise ServicesError(
                f"Error exporting shapefile: {e.output.decode('utf-8')}")

    async def import_file(self, folder, filename, feature_class_name, s_epsg_code, t_epsg_code, split_at_dateline=True, source_feature_class='', where_clause=None):
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
            folder, filename, feature_class_name, s_epsg_code, t_epsg_code, source_feature_class, where_clause)
        logging.debug(f"Running ogr2ogr command: {cmd}")

        result = await run_command(cmd)
        if result != 0:
            raise ServicesError(f"Import failed with return code {result}")

        if split_at_dateline:
            query = f'UPDATE bioprotect.{feature_class_name} SET geometry = bioprotect.ST_SplitAtDateLine(geometry);'
            await self.execute(query)

    async def import_shapefile(self, folder, shapefile, feature_class_name, s_epsg_code="EPSG:4326", t_epsg_code="EPSG:4326", splitAtDateline=True):
        check_zipped_shapefile(folder + shapefile)
        await self.import_file(folder, shapefile, feature_class_name, s_epsg_code, t_epsg_code, splitAtDateline)

    async def import_gml(self, folder, gmlfilename, feature_class_name, s_epsg_code="EPSG:4326", t_epsg_code="EPSG:4326", splitAtDateline=True):
        await self.import_file(folder, gmlfilename, feature_class_name, s_epsg_code, t_epsg_code, splitAtDateline)

    async def import_file_GDBFeatureClass(self, folder, fileGDB, sourceFeatureClass, destFeatureClass, s_epsg_code="EPSG:4326", t_epsg_code="EPSG:4326", splitAtDateline=True, where_clause=None):
        await self.import_file(
            folder, fileGDB, destFeatureClass, s_epsg_code, t_epsg_code, splitAtDateline, sourceFeatureClass, where_clause=where_clause)

    # async def is_valid(self, feature_class_name):
    #     query = f"SELECT DISTINCT ST_IsValid(geometry) FROM bioprotect.{feature_class_name} LIMIT 1;"
    #     result = await self.execute(query, return_format="Array")
    #     if not result[0]['st_isvalid']:
    #         await self.drop_existing_table(feature_class_name)
    #         raise ServicesError("The input shapefile has invalid geometries.")

    async def is_valid(self, feature_class_name):
        # Auto-fix
        await self.execute(f"""
            UPDATE bioprotect.{feature_class_name}
            SET geometry = ST_MakeValid(geometry)
            WHERE NOT ST_IsValid(geometry);
        """)

        # Find an id column if one exists
        id_column = "id"
        cols_df = await self.execute(f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'bioprotect'
            AND table_name = '{feature_class_name}'
            AND column_name ILIKE ANY (ARRAY['id','fid','ogc_fid','objectid'])
            LIMIT 1;
        """, return_format="DataFrame")
        if not cols_df.empty:
            id_column = cols_df.iloc[0, 0]

        invalids = await self.execute(f"""
            SELECT {id_column}, ST_IsValidReason(geometry) AS reason
            FROM bioprotect.{feature_class_name}
            WHERE NOT ST_IsValid(geometry)
            LIMIT 10;
        """, return_format="Array")

        if invalids:
            await self.drop_existing_table(feature_class_name)
            reasons = ", ".join({i["reason"] for i in invalids})
            raise ServicesError(
                f"The file contains invalid geometries: {reasons}")


pg = None


async def get_pg():
    global pg
    if pg is None:
        pg = PostGIS()
        await pg.initialise()
    return pg
