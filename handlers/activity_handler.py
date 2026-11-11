"""
Activity upload and cumulative impact handler.

Supports uploading activities as shapefiles or rasters, and running the
cumulative impact function in PostGIS. An activity is a set of polygons
(where it happens); its pressures come from the PAD table by activity title
at run time - nothing is copied per pressure.
"""

import asyncio
import logging
import os
import time
import uuid
import tempfile

from psycopg2 import sql
from tornado.ioloop import IOLoop

from classes.folder_path_config import get_folder_path_config
from handlers.websocket_handler import SocketHandler
from services.project_service import get_project_crs, off_grid_warning
from services.raster_service import (bounds_wgs84, get_raster_band_info,
                                     large_raster_warning,
                                     raster_presence_to_gpkg)
from services.service_error import ServicesError, raise_error

project_paths = get_folder_path_config()


async def pad_pressure_count(pg, activity_titles):
    """How many PAD pressures (rppscore > 0) these activity titles bring."""
    rows = await pg.execute(
        "SELECT count(*) AS n FROM bioprotect.pad "
        "WHERE activitytitle = ANY(%s) AND rppscore > 0",
        data=[list(activity_titles)], return_format="Array")
    return rows[0]["n"]


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
        The raster file should already be saved to IMPORT_FOLDER (via uploadFileToFolder).
        Send a WebSocket message to trigger the import.
    """

    def initialize(self, pg):
        super().initialize(pg=pg)

    async def open(self):
        try:
            await super().open({'info': "Uploading activity..."})
        except ServicesError as e:
            print('ServicesError as e: ', e)
            return

        try:
            self.validate_args(self.request.arguments,
                               ['activity', 'filename', 'description'])

            activity = self.get_argument('activity')
            filename = self.get_argument('filename')
            description = self.get_argument('description')
            upload_type = self.get_argument('upload_type', 'shapefile')
            # files with no CRS metadata are read in the project CRS; with
            # no project open they are rejected, as before
            self.project_id = self.get_argument('project_id', None)
            self.crs_epsg = await get_project_crs(self.pg, self.project_id)
            if not str(self.project_id or "").isdigit():
                self.project_id = None

            # An activity only matters through its PAD pressures; one with
            # none would be stored and then add nothing to any cost layer.
            pressure_count = await pad_pressure_count(self.pg, [activity])
            if pressure_count == 0:
                # A raster can still become a cost layer directly (values kept);
                # cost profiles don't come from shapefiles, so no hint there.
                hint = (" To use this raster as a cost layer anyway, upload it "
                        "in the 'Upload Raster Cost Profile' tab instead."
                        if upload_type == 'raster' else "")
                raise ServicesError(
                    f"'{activity}' has no pressures in the PAD table, so it "
                    "would not affect any cost layer. Choose another activity."
                    + hint)

            if upload_type == 'shapefile':
                activity_id = await self._import_shapefile(
                    activity, filename, description)
            elif upload_type == 'raster':
                activity_id = await self._import_raster(
                    activity, filename, description)
            else:
                raise ServicesError(
                    f"Unsupported upload_type: {upload_type}. Use 'shapefile' or 'raster'.")

            self.close(close_message={
                'info': f"Activity uploaded. {pressure_count} pressures apply.",
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

        # Use ogr2ogr via the pg class (same as feature imports).
        # s_epsg_code=None lets ogr2ogr autodetect the source CRS from the .prj
        # file, so shapefiles in projections other than WGS84 get reprojected
        # correctly instead of being mis-tagged. Without a .prj, use the
        # project CRS (import_shapefile rejects it when there's no project).
        has_prj = os.path.exists(os.path.join(
            project_paths.IMPORT_FOLDER, filename.rsplit('.', 1)[0] + '.prj'))
        assume_crs = not has_prj and self.project_id
        await self.pg.import_shapefile(
            project_paths.IMPORT_FOLDER,
            filename,
            activity_table,
            s_epsg_code=f"EPSG:{self.crs_epsg}" if assume_crs else None,
        )
        if assume_crs:
            self.send_response({
                'status': 'Preprocessing',
                'info': await off_grid_warning(
                    self.pg, self.project_id, self.crs_epsg, table=activity_table)
                or f"'{filename}' has no .prj file, so it was read as "
                   f"EPSG:{self.crs_epsg} (the project CRS)."
            })

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

        Activities are presence-only: the raster is reprojected to WGS84
        (nearest neighbour), cut to where it is > 0, polygonised by GDAL on
        disk, and the polygons loaded with ogr2ogr. Nothing runs as one big
        pool query, so a large raster is not stopped by the 60s cap, and
        memory stays flat (a numpy read of the raster OOM-killed prod).
        """
        activity_table = get_unique_activity_name()
        raster_path = os.path.join(project_paths.IMPORT_FOLDER, filename)

        if not os.path.isfile(raster_path):
            raise ServicesError(f"Raster file not found: {raster_path}")

        info = get_raster_band_info(raster_path)
        warning = large_raster_warning(info)
        if warning:
            self.send_response({'status': 'Preprocessing', 'info': warning})

        fallback_crs = f"EPSG:{self.crs_epsg}" if self.project_id else None
        if fallback_crs and info["crs_epsg"] is None and info["crs_wkt"] is None:
            self.send_response({
                'status': 'Preprocessing',
                'info': await off_grid_warning(
                    self.pg, self.project_id, self.crs_epsg,
                    bbox=bounds_wgs84(raster_path, fallback_crs))
                or f"The raster has no CRS, so it was read as "
                   f"EPSG:{self.crs_epsg} (the project CRS)."
            })

        # GDAL runs in a worker thread; add_callback is the thread-safe way
        # back onto the IOLoop to report its progress.
        loop = IOLoop.current()

        def progress(stage, pct):
            if pct % 20 == 0 and 0 < pct < 100:
                loop.add_callback(self.send_response, {
                    'status': 'Preprocessing', 'info': f'{stage}... {pct}%'})

        try:
            with tempfile.TemporaryDirectory(
                    dir=project_paths.IMPORT_FOLDER) as work:
                await asyncio.to_thread(
                    raster_presence_to_gpkg, raster_path,
                    os.path.join(work, "presence.gpkg"), fallback_crs, progress)
                self.send_response({
                    'status': 'Preprocessing',
                    'info': 'Loading activity polygons...'
                })
                # ponytail: no dateline split - the old raster path never did
                # one, and an UPDATE over every polygon could hit the 60s cap.
                await self.pg.import_file(work, "presence.gpkg", activity_table,
                                          None, "EPSG:4326",
                                          split_at_dateline=False)
        finally:
            if os.path.exists(raster_path):
                os.remove(raster_path)

        await self.pg.execute(sql.SQL(
            # presence only: no value column; GPKG's fid gives way to our id
            "ALTER TABLE bioprotect.{} DROP COLUMN IF EXISTS dn, "
            "DROP COLUMN IF EXISTS fid"
        ).format(sql.Identifier(activity_table)))
        has_rows = await self.pg.execute(sql.SQL(
            "SELECT EXISTS (SELECT 1 FROM bioprotect.{}) AS any"
        ).format(sql.Identifier(activity_table)), return_format="Array")
        if not has_rows[0]["any"]:
            await self.pg.drop_existing_table(activity_table)
            raise ServicesError(
                "The raster has no pixels above 0, so there is no activity "
                "footprint to save.")

        # Add spatial index and primary key
        await self._finalise_activity_table(activity_table)

        # Insert metadata
        activity_id = await self._insert_metadata(
            activity_table, activity, filename, description, 'raster')

        return activity_id

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
                 filename, activity, activity_name, extent)
            SELECT
                now(), %s, %s, %s, %s, %s, %s,
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
                activity_table
            ],
            return_format="Array"
        )

        return result[0]['id']

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
            return

        try:
            self.validate_args(self.request.arguments,
                               ['project_id', 'activity_ids', 'profile_name'])

            project_id = int(self.get_argument('project_id'))
            activity_ids_str = self.get_argument('activity_ids')
            activity_ids = [int(x.strip())
                            for x in activity_ids_str.split(',')]
            profile_name = self.get_argument('profile_name')
            description = self.get_argument('description', '')

            # Step 0: Check features have been preprocessed
            feature_check = await self.pg.execute(
                """
                SELECT COUNT(DISTINCT pfa.feature_unique_id) AS preprocessed_count,
                       (SELECT COUNT(*) FROM bioprotect.project_features
                        WHERE project_id = %s) AS total_features
                FROM bioprotect.pu_feature_amounts pfa
                WHERE pfa.project_id = %s
                """,
                data=[project_id, project_id],
                return_format="Array"
            )

            preprocessed = feature_check[0]['preprocessed_count']
            total = feature_check[0]['total_features']

            if preprocessed == 0:
                raise ServicesError(
                    "No features have been preprocessed for this project. "
                    "Please preprocess at least one feature before running "
                    "the cumulative impact function."
                )

            if preprocessed < total:
                self.send_response({
                    'status': 'Preprocessing',
                    'info': f'Warning: only {preprocessed} of {total} features '
                            f'have been preprocessed. Unprocessed features will '
                            f'be excluded from the impact calculation.'
                })

            titles = await self.pg.execute(
                "SELECT activity FROM bioprotect.metadata_activities "
                "WHERE id = ANY(%s)", data=[activity_ids], return_format="Array")
            total_pressures = 0
            for t in titles:  # per row: one title can back several activities
                total_pressures += await pad_pressure_count(self.pg, [t["activity"]])

            pu_count = (await self.pg.execute(
                "SELECT COUNT(*) AS n FROM bioprotect.project_pus WHERE project_id = %s;",
                data=[project_id],
                return_format="Array"
            ))[0]['n']

            self.send_response({
                'status': 'Preprocessing',
                'info': f'{total_pressures} pressures apply. Calculating cumulative '
                        f'impact across {pu_count:,} planning units '
                        f'(overlaying pressures, features and sensitivity scores)...'
            })

            # Step 2: Run cumulative impact. It is one SQL statement, so there are
            # no real sub-stages to report - send a heartbeat so the user knows
            # it's still working.
            # ponytail: heartbeat only; split run_cumulative_impact into steps if per-stage progress is needed
            task = asyncio.ensure_future(self.pg.execute(
                "SELECT bioprotect.run_cumulative_impact(%s, %s, %s, %s, %s);",
                data=[
                    project_id,
                    activity_ids,
                    profile_name,
                    description,
                    self.get_current_user()
                ],
                return_format="Array"
            ))
            started = time.monotonic()
            while not task.done():
                await asyncio.wait({task}, timeout=10)
                if not task.done():
                    self.send_response({
                        'status': 'Preprocessing',
                        'info': f'Still calculating cumulative impact '
                                f'({int(time.monotonic() - started)}s)...'
                    })
            result = task.result()

            self.send_response({
                'status': 'Preprocessing',
                'info': 'Cumulative impact calculated. Activating cost profile...'
            })

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
