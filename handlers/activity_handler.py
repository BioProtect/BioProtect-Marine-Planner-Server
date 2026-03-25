"""
Activity upload and cumulative impact handler.

Supports uploading activities as shapefiles or rasters, creating pressures
from the PAD table, and running the cumulative impact function in PostGIS.
"""

import logging
import os
import uuid
import subprocess

import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from psycopg2 import sql

from classes.folder_path_config import get_folder_path_config
from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError, raise_error

project_paths = get_folder_path_config()


def get_unique_activity_name():
    """Generate a unique table name for the activity geometry table."""
    return "activity_" + uuid.uuid4().hex[:23]


class UploadActivityHandler(SocketHandler):
    """
    WebSocket handler for uploading an activity (shapefile or raster).

    Expects query args:
        - activity:    The activity title (must match an entry in the PAD table)
        - filename:    Original filename
        - description: Description text
        - upload_type: 'shapefile' or 'raster'

    For shapefiles:
        The shapefile should already be unzipped in IMPORT_FOLDER (via uploadFileToFolder + unzipShapefile).
        Send a WebSocket message (any content) to trigger the import.

    For rasters:
        The raster file should already be saved to data/tmp/ (via uploadFileToFolder).
        Send a WebSocket message to trigger the import.
    """

    def initialize(self, pg):
        super().initialize(pg=pg)

    async def open(self):
        try:
            await super().open({'info': "Uploading activity..."})
        except ServicesError as e:
            print('ServicesError as e: ', e)
            pass

    async def on_message(self, message):
        try:
            self.validate_args(self.request.arguments,
                               ['activity', 'filename', 'description'])

            activity = self.get_argument('activity')
            filename = self.get_argument('filename')
            description = self.get_argument('description')
            upload_type = self.get_argument('upload_type', 'shapefile')

            if upload_type == 'shapefile':
                activity_id = await self._import_shapefile(
                    activity, filename, description)
            elif upload_type == 'raster':
                activity_id = await self._import_raster(
                    activity, filename, description)
            else:
                raise ServicesError(
                    f"Unsupported upload_type: {upload_type}. Use 'shapefile' or 'raster'.")

            # Create pressures from the uploaded activity
            self.send_response({
                'status': 'Preprocessing',
                'info': 'Creating pressures from activity...'
            })

            pressure_count = await self._create_pressures(activity_id)

            self.close(close_message={
                'info': f"Activity uploaded. {pressure_count} pressures created.",
                'activity_id': activity_id,
                'pressure_count': pressure_count
            })

        except ServicesError as e:
            self.close(close_message={
                'error': e.args[0],
                'info': 'Failed to upload activity'
            })
        except Exception as e:
            logging.error(f"Unexpected error uploading activity: {e}",
                          exc_info=True)
            self.close(close_message={
                'error': str(e),
                'info': 'Failed to upload activity'
            })

    async def _import_shapefile(self, activity, filename, description):
        """Import a shapefile as a vector activity table."""
        activity_table = get_unique_activity_name()

        self.send_response({
            'status': 'Preprocessing',
            'info': 'Importing shapefile into database...'
        })

        # Use ogr2ogr via the pg class (same as feature imports)
        await self.pg.import_shapefile(
            project_paths.IMPORT_FOLDER,
            filename,
            activity_table
        )

        # Validate and fix geometries
        self.send_response({
            'status': 'Preprocessing',
            'info': 'Validating geometries...'
        })
        await self.pg.is_valid(activity_table)

        # Add spatial index and primary key
        await self._finalise_activity_table(activity_table)

        # Insert metadata
        activity_id = await self._insert_metadata(
            activity_table, activity, filename, description, 'shapefile')

        return activity_id

    async def _import_raster(self, activity, filename, description):
        """
        Import a raster as a vector activity table.

        Loads the raster, reprojects to WGS84, polygonizes it, and stores
        the resulting polygons as vector geometry in PostGIS.
        """
        activity_table = get_unique_activity_name()
        raster_path = os.path.join('data/tmp', filename)

        if not os.path.isfile(raster_path):
            raise ServicesError(f"Raster file not found: {raster_path}")

        self.send_response({
            'status': 'Preprocessing',
            'info': 'Reprojecting raster to WGS84...'
        })

        # Reproject to WGS84 if needed, then polygonize via PostGIS
        reprojected_path = os.path.join('data/tmp', f'repro_{filename}')
        self._reproject_to_wgs84(raster_path, reprojected_path)

        self.send_response({
            'status': 'Preprocessing',
            'info': 'Loading raster into temporary table...'
        })

        # Load raster into a temporary PostGIS raster table
        temp_raster_table = f"tmp_rast_{uuid.uuid4().hex[:16]}"
        db_config = self.pg.config

        raster2pgsql_cmd = [
            "raster2pgsql", "-s", "4326", "-d", "-I", "-C", "-F",
            reprojected_path, f"bioprotect.{temp_raster_table}"
        ]

        psql_cmd = [
            "psql", db_config.build_connection_string()
        ]

        try:
            raster_proc = subprocess.Popen(
                raster2pgsql_cmd, stdout=subprocess.PIPE)
            subprocess.run(psql_cmd, stdin=raster_proc.stdout, check=True)
            raster_proc.stdout.close()
        except (subprocess.CalledProcessError, TypeError) as e:
            raise ServicesError(
                f"Failed to load raster into database: {e}")

        self.send_response({
            'status': 'Preprocessing',
            'info': 'Polygonizing raster...'
        })

        # Polygonize: convert raster to vector geometry table
        await self.pg.execute(
            sql.SQL("""
                CREATE TABLE bioprotect.{activity_table} AS
                SELECT (ST_DumpAsPolygons(rast)).geom AS geometry,
                       (ST_DumpAsPolygons(rast)).val  AS value
                FROM bioprotect.{temp_raster_table}
                WHERE (ST_DumpAsPolygons(rast)).val > 0;
            """).format(
                activity_table=sql.Identifier(activity_table),
                temp_raster_table=sql.Identifier(temp_raster_table)
            )
        )

        # Drop temp raster table
        await self.pg.execute(
            sql.SQL("DROP TABLE IF EXISTS bioprotect.{};").format(
                sql.Identifier(temp_raster_table)
            )
        )

        # Clean up temp files
        for f in [raster_path, reprojected_path]:
            if os.path.exists(f):
                os.remove(f)

        # Add spatial index and primary key
        await self._finalise_activity_table(activity_table)

        # Insert metadata
        activity_id = await self._insert_metadata(
            activity_table, activity, filename, description, 'raster')

        return activity_id

    def _reproject_to_wgs84(self, input_path, output_path):
        """Reproject a raster file to EPSG:4326 if not already."""
        with rasterio.open(input_path) as src:
            if src.crs and src.crs.to_epsg() == 4326:
                # Already WGS84, just copy
                if input_path != output_path:
                    import shutil
                    shutil.copy2(input_path, output_path)
                return

            transform, width, height = calculate_default_transform(
                src.crs, 'EPSG:4326', src.width, src.height, *src.bounds)

            kwargs = src.meta.copy()
            kwargs.update({
                'crs': 'EPSG:4326',
                'transform': transform,
                'width': width,
                'height': height
            })

            with rasterio.open(output_path, 'w', **kwargs) as dst:
                for i in range(1, src.count + 1):
                    reproject(
                        source=rasterio.band(src, i),
                        destination=rasterio.band(dst, i),
                        src_transform=src.transform,
                        src_crs=src.crs,
                        dst_transform=transform,
                        dst_crs='EPSG:4326',
                        resampling=Resampling.bilinear
                    )

    async def _finalise_activity_table(self, activity_table):
        """Add spatial index and primary key to the activity geometry table."""
        index_name = f"idx_{uuid.uuid4().hex}"

        # Spatial index
        await self.pg.execute(
            sql.SQL("CREATE INDEX {} ON bioprotect.{} USING GIST (geometry);")
            .format(sql.Identifier(index_name), sql.Identifier(activity_table))
        )

        # Primary key
        try:
            await self.pg.execute(
                sql.SQL(
                    "ALTER TABLE bioprotect.{} "
                    "DROP COLUMN IF EXISTS id, DROP COLUMN IF EXISTS ogc_fid;"
                ).format(sql.Identifier(activity_table))
            )
            await self.pg.execute(
                sql.SQL(
                    "ALTER TABLE bioprotect.{} ADD COLUMN id SERIAL PRIMARY KEY;"
                ).format(sql.Identifier(activity_table))
            )
        except Exception as e:
            logging.warning(
                f"Primary key may already exist for {activity_table}: {e}")

    async def _insert_metadata(self, activity_table, activity, filename,
                               description, upload_type):
        """Insert a metadata record and return the new activity id."""
        query = sql.SQL("""
            INSERT INTO bioprotect.metadata_activities
                (creation_date, description, source, created_by,
                 filename, activity, activity_name, upload_type, extent)
            SELECT
                now(), %s, %s, %s, %s, %s, %s, %s,
                Box2D(ST_Extent(geometry))
            FROM bioprotect.{activity_table}
            RETURNING id;
        """).format(activity_table=sql.Identifier(activity_table))

        result = await self.pg.execute(
            query,
            data=[
                description,
                upload_type,
                self.get_current_user(),
                filename.lower(),
                activity,
                activity_table,
                upload_type
            ],
            return_format="Array"
        )

        return result[0]['id']

    async def _create_pressures(self, activity_id):
        """Call the PostGIS function to create pressures from the activity."""
        result = await self.pg.execute(
            "SELECT bioprotect.create_pressures_from_activity(%s);",
            data=[activity_id],
            return_format="Array"
        )
        return result[0]['create_pressures_from_activity']


class RunCumulativeImpactHandler(SocketHandler):
    """
    WebSocket handler for running the cumulative impact function.

    Expects query args:
        - project_id:   The project to compute for
        - activity_ids: Comma-separated list of activity IDs to include
        - profile_name: Name for the cost profile
        - description:  Optional description
    """

    def initialize(self, pg):
        super().initialize(pg=pg)

    async def open(self):
        try:
            await super().open({
                'info': "Running Cumulative Impact..."
            })
        except ServicesError as e:
            print('ServicesError as e: ', e)
            pass

    async def on_message(self, message):
        try:
            self.validate_args(self.request.arguments,
                               ['project_id', 'activity_ids', 'profile_name'])

            project_id = int(self.get_argument('project_id'))
            activity_ids_str = self.get_argument('activity_ids')
            activity_ids = [int(x.strip())
                            for x in activity_ids_str.split(',')]
            profile_name = self.get_argument('profile_name')
            description = self.get_argument('description', '')

            # Step 1: Create/refresh pressures for all selected activities
            self.send_response({
                'status': 'Preprocessing',
                'info': 'Creating pressures from activities...'
            })

            total_pressures = 0
            for aid in activity_ids:
                result = await self.pg.execute(
                    "SELECT bioprotect.create_pressures_from_activity(%s);",
                    data=[aid],
                    return_format="Array"
                )
                count = result[0]['create_pressures_from_activity']
                total_pressures += count

            self.send_response({
                'status': 'Preprocessing',
                'info': f'{total_pressures} pressures created. Running cumulative impact...'
            })

            # Step 2: Run cumulative impact
            result = await self.pg.execute(
                "SELECT bioprotect.run_cumulative_impact(%s, %s, %s, %s, %s);",
                data=[
                    project_id,
                    activity_ids,
                    profile_name,
                    description,
                    self.get_current_user()
                ],
                return_format="Array"
            )

            cost_profile_id = result[0]['run_cumulative_impact']

            # Optionally set as active cost profile
            set_active = self.get_argument('set_active', 'false')
            if set_active.lower() == 'true':
                await self.pg.execute(
                    "UPDATE bioprotect.projects SET active_cost_profile_id = %s WHERE id = %s;",
                    data=[cost_profile_id, project_id]
                )

            self.close(close_message={
                'info': 'Cumulative impact calculation complete.',
                'cost_profile_id': cost_profile_id,
                'total_pressures': total_pressures
            })

        except ServicesError as e:
            self.close(close_message={
                'error': e.args[0],
                'info': 'Failed to run cumulative impact'
            })
        except Exception as e:
            logging.error(
                f"Unexpected error in cumulative impact: {e}", exc_info=True)
            self.close(close_message={
                'error': str(e),
                'info': 'Failed to run cumulative impact'
            })
