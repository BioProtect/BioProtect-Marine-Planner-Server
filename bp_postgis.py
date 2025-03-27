import aiopg
import psycopg2
from psycopg2 import sql
import pandas as pd
import logging
import uuid
from subprocess import CalledProcessError
from typing import List, Optional, Union


class PostGIS:
    """Utility class for interacting with PostGIS databases, executing queries, and handling spatial data imports/exports.

    Attributes:
        pool (aiopg.Pool): Connection pool for PostGIS.
    """

    def __init__(self, connection_string: str, minsize: int = 1, maxsize: int = 10):
        """Initialize the PostGIS class with a connection string and pool size parameters.

        Args:
            connection_string (str): PostgreSQL connection string.
            minsize (int): Minimum number of connections in the pool.
            maxsize (int): Maximum number of connections in the pool.
        """
        self.connection_string = connection_string
        self.minsize = minsize
        self.maxsize = maxsize
        self.pool = None

    async def initialise(self):
        """Initialize the connection pool to the PostGIS database."""
        self.pool = await aiopg.create_pool(self.connection_string, timeout=None, minsize=self.minsize, maxsize=self.maxsize)

    async def execute(self, sql_query: str, data: Optional[List] = None, return_format: Optional[str] = None, filename: Optional[str] = None):
        """Execute an SQL query and optionally return records or write to a file.

        Args:
            sql_query (str): SQL string to execute.
            data (list, optional): Parameters to substitute into the query.
            return_format (str, optional): If the query returns results, specifies the return format (Array, DataFrame, Dict, or File).
            filename (str, optional): If return_format is 'File', the output CSV file path.

        Returns:
            Union[List, pd.DataFrame, dict]: The result of the query, depending on the return format.

        Raises:
            Exception: Raises any errors encountered during query execution.
        """
        async with self.pool.acquire() as conn:
            async with conn.cursor() as cur:
                sql_query = cur.mogrify(sql_query, data) if data else sql_query
                await cur.execute(sql_query)

                if not return_format:
                    return

                records = await cur.fetchall()
                if return_format == "Array":
                    return records
                else:
                    columns = [desc[0] for desc in cur.description]
                    df = pd.DataFrame.from_records(records, columns=columns)
                    if return_format == "DataFrame":
                        return df
                    elif return_format == "Dict":
                        return df.to_dict(orient="records")
                    elif return_format == "File" and filename:
                        df.to_csv(filename, index=False)

    async def import_file(self, folder: str, filename: str, feature_class_name: str, s_epsg: str, t_epsg: str, ogr_executable: str, split_at_dateline: bool = True):
        """Imports a file into PostGIS using ogr2ogr.

        Args:
            folder (str): Path to the file location.
            filename (str): Name of the file to import.
            feature_class_name (str): Name of the destination feature class.
            s_epsg (str): Source EPSG code.
            t_epsg (str): Target EPSG code.
            ogr_executable (str): Path to ogr2ogr executable.
            split_at_dateline (bool): If true, splits features at the dateline.

        Raises:
            Exception: If the import fails.
        """
        cmd = f'{ogr_executable} -f "PostgreSQL" PG:"{self.connection_string}" "{folder}/{
            filename}" -nlt GEOMETRY -lco SCHEMA=public -lco GEOMETRY_NAME=geometry -nln {feature_class_name} -s_srs {s_epsg} -t_srs {t_epsg} -lco precision=NO'
        result = await self._run_cmd(cmd)

        if result != 0:
            raise Exception(f"Import failed with return code {result}")

        if split_at_dateline:
            await self.execute(f"UPDATE public.{feature_class_name} SET geometry = ST_SplitAtDateLine(geometry);")

    async def _run_cmd(self, cmd: str) -> int:
        """Runs a command using subprocess and returns the result code.

        Args:
            cmd (str): Command to run.

        Returns:
            int: Result code of the command.
        """
        try:
            logging.debug(f'Running command: {cmd}')
            result = await self.execute(cmd)
            return result
        except CalledProcessError as e:
            logging.error(f'Command failed: {e}')
            raise e

    async def get_geometry_type(self, feature_class_name: str) -> str:
        """Gets the geometry type of a feature class.

        Args:
            feature_class_name (str): Name of the feature class.

        Returns:
            str: The PostGIS geometry type (e.g., 'ST_Point', 'ST_Polygon').
        """
        result = await self.execute(f"SELECT ST_GeometryType(geometry) FROM public.{feature_class_name} LIMIT 1;", return_format="Array")
        return result[0][0]

    async def create_primary_key(self, feature_class_name: str, column: str):
        """Creates a primary key on the specified column of a feature class.

        Args:
            feature_class_name (str): Name of the feature class.
            column (str): Column to use as the primary key.
        """
        key_name = f"pk_{uuid.uuid4().hex}"
        await self.execute(f"ALTER TABLE public.{feature_class_name} ADD CONSTRAINT {key_name} PRIMARY KEY ({column});")

    async def is_valid(self, feature_class_name: str):
        """Checks if geometries in a feature class are valid, and deletes the class if not.

        Args:
            feature_class_name (str): Name of the feature class to validate.

        Raises:
            Exception: If invalid geometries are found.
        """
        result = await self.execute(f"SELECT DISTINCT ST_IsValid(geometry) FROM public.{feature_class_name} LIMIT 1;", return_format="Array")
        if not result[0][0]:
            await self.execute(f"DROP TABLE IF EXISTS public.{feature_class_name};")
            raise Exception(f"Feature class {
                            feature_class_name} has invalid geometries.")

# Example usage
# pg = PostGIS(connection_string="dbname=mydb user=myuser password=mypassword host=myhost")
# await pg.initialise()
