import os
import requests
from psycopg2 import sql
from tornado.ioloop import IOLoop
from services.file_service import write_to_file
from services.service_error import ServicesError, raise_error
from handlers.websocket_handler import SocketHandler
import uuid


class FeatureHandler(SocketHandler):
    """
    WebSocket Handler for feature-related operations, including importing features from shapefiles
    or WFS endpoints.
    """

    def initialize(self, pg, finish_feature_import, upload_tileset_to_mapbox):
        super().initialize()
        self.pg = pg
        self.finish_feature_import = finish_feature_import
        self.upload_tileset_to_mapbox = upload_tileset_to_mapbox

    async def open(self):
        try:
            await super().open({'info': "Processing features.."})
        except ServicesError:
            pass

    async def on_message(self, message):
        action = self.get_argument('action', None)

        try:
            if action == 'import_shapefile':
                await self.import_features_from_shapefile()
            elif action == 'import_wfs':
                await self.import_features_from_wfs()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def import_features_from_shapefile(self):
        """Imports features from a shapefile."""
        self.validate_args(self.request.arguments, ['shapefile'])
        shapefile = self.get_argument('shapefile')
        upload_ids = []
        scratch_name = "scratch_" + uuid.uuid4().hex[:24]
        name = self.get_argument('name', None)

        try:
            # Import shapefile into PostGIS
            await self.pg.importShapefile(self.proj_paths.IMPORT_FOLDER, shapefile, scratch_name)
            self.send_response(
                {'status': 'Preprocessing', 'info': "Checking the geometry.."})
            await self.pg.isValid(scratch_name)

            # Handle feature names
            feature_names = [name] if name else await self.get_feature_names_from_shapefile(scratch_name)

            for feature_name in feature_names:

                feature_class_name = "f_" + \
                    uuid.uuid4().hex[:30] if name else "fs_" + \
                    uuid.uuid4().hex[:29]
                await self.create_feature_class_from_shapefile(scratch_name, feature_class_name, feature_name, name)
                upload_id = await self.finalize_feature_import(feature_class_name, feature_name)
                upload_ids.append(upload_id)

            self.close({'info': "Features imported", 'uploadIds': upload_ids})

        except ServicesError as e:
            self.handle_import_error(e, feature_name)
        finally:
            await self.cleanup_scratch(scratch_name)

    async def import_features_from_wfs(self):
        """Imports features from a WFS endpoint."""
        self.validate_args(self.request.arguments, [
                           'srs', 'endpoint', 'name', 'description', 'featuretype'])
        feature_class_name = "f_" + uuid.uuid4().hex[:30]
        endpoint = self.get_argument('endpoint')
        feature_type = self.get_argument('featuretype')

        try:
            # Fetch GML data
            gml = await IOLoop.current().run_in_executor(None, self.get_gml, endpoint, feature_type)
            gml_path = os.path.join(self.proj_paths.IMPORT_FOLDER, f"{
                                    feature_class_name}.gml")
            write_to_file(gml_path, gml)

            # Import GML into PostGIS
            await self.pg.importGml(self.proj_paths.IMPORT_FOLDER, f"{feature_class_name}.gml", feature_class_name, sEpsgCode=self.get_argument('srs'))
            self.send_response(
                {'status': 'Preprocessing', 'info': "Checking the geometry.."})
            await self.pg.isValid(feature_class_name)

            # Finalize feature import
            feature_id = await self.finalize_feature_import(feature_class_name, self.get_argument('name'))
            self.close({'info': "Features imported", 'uploadId': feature_id})

        except ServicesError as e:
            self.handle_import_error(e, self.get_argument('name'))
        finally:
            self.cleanup_gml_files(feature_class_name)

    async def get_feature_names_from_shapefile(self, scratch_name):
        split_field = self.get_argument('splitfield')

        query = sql.SQL("""
            SELECT DISTINCT {split_field}
            FROM marxan.{scratch_table}
        """).format(
            split_field=sql.Identifier(split_field),
            scratch_table=sql.Identifier(scratch_name)
        )

        features_df = await self.pg.execute(query, return_format="DataFrame")
        return features_df[split_field].dropna().tolist()

    async def create_feature_class_from_shapefile(self, scratch_name, feature_class_name, feature_name, single_name):
        query = """
            CREATE TABLE marxan.{feature_class_name} AS 
            SELECT * FROM marxan.{scratch_table} {condition};
        """
        if single_name:
            condition = ""
        else:
            split_field = self.get_argument('splitfield')
            condition = f"WHERE {split_field} = %s"

        await self.pg.execute(
            sql.SQL(query).format(
                feature_class_name=sql.Identifier(feature_class_name),
                scratch_table=sql.Identifier(scratch_name),
                condition=sql.SQL(condition)
            ),
            [feature_name] if not single_name else []
        )

    async def finalize_feature_import(self, feature_class_name, feature_name):
        description = self.get_argument(
            'description', f"Imported from '{feature_name}'")
        geometry_type = await self.pg.getGeometryType(feature_class_name)

        source = "Imported shapefile (points)" if geometry_type == 'ST_Point' else "Imported shapefile"

        # Finalize the feature import
        feature_id = await self.finish_feature_import(
            feature_class_name=feature_class_name,
            feature_name=feature_name,
            description=description,
            source=source,
            user=self.get_current_user()
        )

        upload_id = await self.upload_tileset_to_mapbox(feature_class_name, feature_class_name)

        # Send a response to the client
        self.send_response({
            'id': feature_id,
            'feature_class_name': feature_class_name,
            'uploadId': upload_id,
            'status': 'FeatureCreated'
        })

        return upload_id

    @staticmethod
    def get_gml(endpoint, featuretype):
        response = requests.get(
            f"{endpoint}&request=getfeature&typeNames={featuretype}")
        response.raise_for_status()
        return response.text

    async def handle_import_error(self, error, feature_name):
        if "already exists" in error.args[0]:
            self.close({'error': f"The feature '{
                       feature_name}' already exists", 'info': 'Failed to import features'})
        else:
            self.close(
                {'error': error.args[0], 'info': 'Failed to import features'})

    async def cleanup_scratch(self, scratch_name):
        await self.pg.execute(sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(sql.Identifier(scratch_name)))

    def cleanup_gml_files(self, feature_class_name):
        for ext in [".gml", ".gfs"]:
            file_path = os.path.join(self.proj_paths.IMPORT_FOLDER, f"{
                                     feature_class_name}{ext}")
            if os.path.exists(file_path):
                os.remove(file_path)
