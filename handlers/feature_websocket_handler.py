"""WebSocket handler for importing features from an uploaded shapefile.

Moved out of app.py to keep the route module from growing without bound. The
route, its query-string protocol and its WebSocket message shape are unchanged,
so the frontend (App.jsx importFeatures -> WebSocketHandler.jsx) needs no edits.
"""

import os

from psycopg2 import sql

from classes.folder_path_config import get_folder_path_config
from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError


# Some source datasets record genuine survey uncertainty by packing two
# habitat classes into a single attribute value, e.g.
# 'Upper bathyal sediment or Upper bathyal rock and biogenic reef'.
# Those polygons belong to BOTH classes, so the split treats this token as a
# delimiter rather than creating a hybrid feature class for the literal string.
MULTI_VALUE_DELIMITER = r'\s+or\s+'

# Guard against a numeric/ID field being chosen as the split field by mistake,
# which would otherwise silently create hundreds of near-empty feature classes.
MAX_SPLIT_FEATURES = 100


class ImportFeaturesWSHandler(SocketHandler):
    """REST WebSocket Handler. Imports a set of features from an unzipped shapefile. This can either be a single feature class or multiple. Sends an error if the feature(s) already exist(s). The required arguments in the request.arguments parameter are:

    Args:
        shapefile (string): The name of shapefile to import (minus the *.shp extension).
        name (string): Optional. If specified then this is the name of the single feature class that will be imported. If omitted then the import is for multiple features.
        description (string): Optional. A description for the imported feature class.
        splitfield (string): Optional. The name of the field to use to split the features in the shapefile into separate feature classes. The separate feature classes will have a name derived from the values in this field.
    Returns:
        WebSocket dict messages with one or more of the following keys (if the class raises an exception, the error message is included in an 'error' key/value pair):

        {
            "info": Contains detailed progress statements on the import process,
            "elapsedtime": The elapsed time in seconds of the run,
            "status": One of Preprocessing, pid, FeatureCreated or Finished,
            "id": The oid of the feature created,
            "feature_class_name": The name of the feature class created,
            "uploadId": The Mapbox tileset upload id (for a single feature),
            "uploadIds": string[]: The Mapbox tileset upload ids (for multiple feature)
        }
    """

    def initialize(self, pg, finish_feature_import, get_unique_feature_name):
        """Collaborators are injected from the route table rather than imported,
        because they live in app.py and importing them here would be circular."""
        super().initialize(pg=pg)
        self.proj_paths = get_folder_path_config()
        self.finish_feature_import = finish_feature_import
        self.get_unique_feature_name = get_unique_feature_name

    async def open(self):
        try:
            await super().open({'info': "Importing features.."})

        except ServicesError:  # authentication/authorisation error
            pass
        else:
            # validate the input arguments
            self.validate_args(self.request.arguments, ['shapefile'])
            # get the name of the shapefile that has already been unzipped on the server
            shapefile = self.get_argument('shapefile')
            # if a name is passed then this is a single feature class
            if "name" in list(self.request.arguments.keys()):
                name = self.get_argument('name')
            else:
                name = None
            scratch_name = None
            try:
                # get a scratch name for the import
                scratch_name = self.get_unique_feature_name("scratch_")
                # Autodetect the source CRS from the shapefile's .prj wherever
                # one exists. Passing an explicit source EPSG makes ogr2ogr emit
                # -s_srs, which OVERRIDES the .prj and silently corrupts the
                # geometry of any projected input (e.g. an Albers equal-area
                # file whose coordinates are metres rather than degrees).
                # A .prj is not required by check_zipped_shapefile, so when one
                # is absent fall back to the historic EPSG:4326 assumption -
                # but say so, rather than guessing silently.
                prj_path = os.path.join(
                    self.proj_paths.IMPORT_FOLDER,
                    shapefile.rsplit('.', 1)[0] + '.prj')
                has_prj = os.path.exists(prj_path)
                if not has_prj:
                    self.send_response({
                        'status': 'Preprocessing',
                        'info': (
                            f"'{shapefile}' has no .prj file, so its coordinates "
                            "are assumed to be WGS84 (EPSG:4326). If the features "
                            "appear in the wrong place on the map, re-export the "
                            "shapefile with a .prj file and import it again."
                        )
                    })
                await self.pg.import_shapefile(
                    self.proj_paths.IMPORT_FOLDER, shapefile, scratch_name,
                    s_epsg_code=None if has_prj else "EPSG:4326")
                # check the geometry
                self.send_response({
                    'status': 'Preprocessing',
                    'info': "Checking the geometry.."
                })
                await self.pg.is_valid(scratch_name)

                # get the feature names
                if name:  # single feature name
                    feature_names = [name]
                else:  # get the feature names from a field in the shapefile
                    splitfield = self.get_argument('splitfield')

                    # Rows with no value in the split field cannot belong to any
                    # feature class. Count them up front so the omission is
                    # reported to the user instead of the geometry just vanishing.
                    skipped_query = sql.SQL("""
                        SELECT count(*) AS n
                        FROM bioprotect.{scratch_table}
                        WHERE {split_field} IS NULL
                           OR btrim({split_field}::text) = '';
                    """).format(
                        scratch_table=sql.Identifier(scratch_name),
                        split_field=sql.Identifier(splitfield)
                    )
                    skipped = await self.pg.execute(
                        skipped_query, return_format="Array")
                    skipped_count = int(skipped[0]['n']) if skipped else 0

                    # Derive the names in the database so that NULLs, padding and
                    # compound 'A or B' values are normalised in exactly the same
                    # way here as in the WHERE clause that populates each table.
                    names_query = sql.SQL("""
                        SELECT DISTINCT btrim(v) AS feature_name
                        FROM bioprotect.{scratch_table},
                             LATERAL unnest(
                                 regexp_split_to_array({split_field}::text, %s)
                             ) AS v
                        WHERE {split_field} IS NOT NULL
                          AND btrim(v) <> ''
                        ORDER BY feature_name;
                    """).format(
                        scratch_table=sql.Identifier(scratch_name),
                        split_field=sql.Identifier(splitfield)
                    )
                    features = await self.pg.execute(
                        names_query, [MULTI_VALUE_DELIMITER],
                        return_format="DataFrame")
                    feature_names = features['feature_name'].tolist()

                    if not feature_names:
                        raise ServicesError(
                            f"The field '{splitfield}' has no usable values to "
                            "split on - every row is empty."
                        )
                    if len(feature_names) > MAX_SPLIT_FEATURES:
                        raise ServicesError(
                            f"The field '{splitfield}' has {len(feature_names)} "
                            f"distinct values, more than the limit of "
                            f"{MAX_SPLIT_FEATURES}. This usually means an ID or "
                            "measurement field was picked by mistake - choose a "
                            "field that describes the feature type instead."
                        )

                    if skipped_count:
                        self.send_response({
                            'status': 'Preprocessing',
                            'info': (
                                f"Skipping {skipped_count} row(s) with no value "
                                f"in '{splitfield}'"
                            )
                        })
                    self.send_response({
                        'status': 'Preprocessing',
                        'info': (
                            f"Splitting '{splitfield}' into "
                            f"{len(feature_names)} feature(s)"
                        )
                    })
                # split the imported feature class into separate feature classes
                for feature_name in feature_names:
                    # create the new feature class
                    is_single = bool(name)
                    prefix = "f_" if is_single else "fs_"
                    feature_class_name = self.get_unique_feature_name(prefix)

                    # single feature vs shapefile with multiple features
                    if is_single:
                        # No WHERE clause for single import
                        query = sql.SQL("""
                            CREATE TABLE bioprotect.{feature_class_name} AS
                            SELECT * FROM bioprotect.{scratch_table};
                        """).format(
                            feature_class_name=sql.Identifier(
                                feature_class_name),
                            scratch_table=sql.Identifier(scratch_name)
                        )
                        params = None
                        description = self.get_argument("description")
                    else:
                        # Match on the split field's normalised parts rather than
                        # the literal value, so a polygon tagged 'A or B' is
                        # imported into BOTH feature class A and feature class B.
                        # A plain '=' would instead strand it in a hybrid class
                        # and leave A and B each missing those polygons.
                        query = sql.SQL("""
                            CREATE TABLE bioprotect.{feature_class_name} AS
                            SELECT s.* FROM bioprotect.{scratch_table} AS s
                            WHERE EXISTS (
                                SELECT 1
                                FROM unnest(
                                    regexp_split_to_array(
                                        s.{split_field}::text, %s)
                                ) AS v
                                WHERE btrim(v) = %s
                            );
                        """).format(
                            feature_class_name=sql.Identifier(
                                feature_class_name),
                            scratch_table=sql.Identifier(scratch_name),
                            split_field=sql.Identifier(splitfield)
                        )
                        params = [MULTI_VALUE_DELIMITER, feature_name]
                        description = f"Imported from '{shapefile}' and split by '{splitfield}' field"

                    await self.pg.execute(query, params)

                    # add an index and a record in the metadata_interest_features table and start the upload to mapbox
                    geometryType = await self.pg.get_geometry_type(feature_class_name)
                    source = "Imported shapefile" if (
                        geometryType != 'ST_Point') else "Imported shapefile (points)"

                    id = await self.finish_feature_import(feature_class_name,
                                                          feature_name,
                                                     description,
                                                     source,
                                                     self.get_current_user())
                    self.send_response({
                        'id': id,
                        'feature_class_name': feature_class_name,
                        'info': f"Feature '{feature_name}' imported",
                        'status': 'FeatureCreated'
                    })
                # complete
                self.close({'info': "Features imported"})

            except ServicesError as e:
                self.send_response({
                    'status': 'Finished',
                    'error': str(e),
                    'info': 'Failed to import features'
                })
                self.close(clean=False)
            finally:
                # delete the scratch feature class
                if scratch_name:
                    query = f'DROP TABLE IF EXISTS bioprotect."{scratch_name}"'
                    await self.pg.execute(query)


