import os
import uuid

import pandas as pd
import requests
from handlers.base_handler import BaseHandler
from psycopg2 import sql
from services.file_service import (create_zipfile, file_to_df,
                                   get_key_values_from_file, write_to_file)
from services.project_service import get_projects_for_feature
from services.service_error import ServicesError, raise_error


class FeatureHandler(BaseHandler):
    """
    REST HTTP handler for feature-related operations, including creation, deletion, import, export,
    fetching, and listing projects for a feature.
    """

    def initialize(self, pg, finish_feature_import):
        super().initialize()
        self.pg = pg
        self.finish_feature_import = finish_feature_import

    def validate_args(self, args, required_keys):
        # sourcery skip: use-named-expression
        """Checks that all of the arguments in argumentList are in the arguments dictionary."""
        missing = [key for key in required_keys if key not in args]
        if missing:
            raise ServicesError(f"Missing required arguments: {
                                ', '.join(missing)}")

    async def get(self):
        """
        Handles GET requests for various feature-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'get':
                await self.get_feature()
            elif action == 'delete':
                await self.delete_feature()
            elif action == 'export':
                await self.export_feature()
            elif action == 'list_projects':
                self.list_projects_for_feature()
            elif action == 'planning_units':
                await self.get_feature_planning_units()
            elif action == 'get_sensitivities':
                await self.get_sensitivities()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def post(self):
        """
        Handles POST requests for feature-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'create_from_linestring':
                await self.create_feature_from_linestring()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def get_feature(self):
        """Fetches feature information from PostGIS."""
        self.validate_args(self.request.arguments, ['unique_id'])
        unique_id = self.get_argument("unique_id")

        query = (
            """
            SELECT unique_id::integer AS id, feature_class_name, alias, description,
            _area AS area, extent, to_char(creation_date, 'DD/MM/YY HH24:MI:SS') AS creation_date,
            tilesetid, source, created_by
            FROM bioprotect.metadata_interest_features
            WHERE unique_id = %s;
            """
        )

        data = await self.pg.execute(query, data=[unique_id], return_format="DataFrame")
        self.send_response({"data": data.to_dict(orient="records")})

    async def delete_feature(self):
        """Deletes a feature class and its associated metadata record."""
        self.validate_args(self.request.arguments, ['feature_name'])
        feature_class_name = self.get_argument('feature_name')

        feature_data = await self.pg.execute(
            """
            SELECT unique_id, created_by FROM bioprotect.metadata_interest_features WHERE feature_class_name = %s;
            """,
            data=[feature_class_name],
            return_format="Dict"
        )

        if not feature_data:
            return

        if feature_data[0].get("created_by") == "global admin":
            raise ServicesError(
                "This is a system feature and cannot be deleted.")

        projects = get_projects_for_feature(
            feature_data[0]["unique_id"], self.proj_paths.USERS_FOLDER)
        if projects:
            raise ServicesError(
                "The feature cannot be deleted as it is used in one or more projects.")

        await self.pg.execute(sql.SQL("DROP TABLE IF EXISTS bioprotect.{};").format(sql.Identifier(feature_class_name)))
        await self.pg.execute("DELETE FROM bioprotect.metadata_interest_features WHERE feature_class_name = %s;", [feature_class_name])

        try:
            response = requests.delete(
                f"https://api.mapbox.com/tilesets/v1/{self.proj_paths.MAPBOX_USER}.{
                    feature_class_name}?access_token={self.proj_paths.MBAT}"
            )
            if response.status_code != 204:
                raise ServicesError(f"Failed to delete tileset: {
                                    response.status_code} - {response.text}")

        except Exception as e:
            print(f"Warning: Unable to delete tileset for feature '{
                  feature_class_name}': {e}")

        self.send_response({'info': "Feature deleted"})

    async def export_feature(self):
        """Exports a feature to a shapefile and zips it."""
        self.validate_args(self.request.arguments, ['name'])
        feature_class_name = self.get_argument('name')
        folder = self.proj_paths.EXPORT_FOLDER

        await self.pg.exportToShapefile(folder, feature_class_name, tEpsgCode="EPSG:4326")
        zipfilename = create_zipfile(folder, feature_class_name)

        self.send_response({
            'info': f"Feature '{feature_class_name}' exported",
            'filename': zipfilename
        })

    async def create_feature_from_linestring(self):
        """Creates a new feature from a provided linestring."""
        self.validate_args(self.request.arguments, [
                           'name', 'description', 'linestring'])

        user = self.get_current_user()
        name = self.get_argument('name')
        description = self.get_argument('description')
        linestring = self.get_argument('linestring')
        feature_class_name = "f_" + uuid.uuid4().hex[:30]

        create_table_query = sql.SQL(
            """
            CREATE TABLE bioprotect.{} AS
            SELECT bioprotect.ST_SplitAtDateLine(ST_SetSRID(ST_MakePolygon(%s)::geometry, 4326)) AS geometry;
            """
        ).format(sql.Identifier(feature_class_name))

        await self.pg.execute(create_table_query, [linestring])
        feature_id = await self.finish_feature_import(feature_class_name, name, description, "Drawn on screen", user)

        self.send_response({
            'info': f"Feature '{name}' created",
            'id': feature_id,
            'feature_class_name': feature_class_name,
        })

    async def get_feature_planning_units(self):
        """Gets the planning unit IDs for a feature."""

        self.validate_args(self.request.arguments, ['user', 'project', 'oid'])
        # unique_ids = self.get_argument("oid")
        ids = self.get_argument("unique_id")

        file_name = os.path.join(self.input_folder,
                                 self.projectData["files"]["PUVSPRNAME"])
        df = pd.read_csv(file_name, sep=None, engine='python') if os.path.exists(
            file_name) else pd.DataFrame()
        puids = df.loc[df['species'] == int(ids)]['pu'].unique().tolist()

        self.send_response({"data": puids})

    async def get_sensitivities(self):
        sensitivities = await self.pg.execute("""
            SELECT DISTINCT ON (eunis_code_assessment)
                eunis_code_assessment,
                jncc_habitat
            FROM bioprotect.jncc_sensitivities
            WHERE eunis_code_assessment IS NOT NULL
            ORDER BY eunis_code_assessment, jncc_habitat;
        """, return_format="Array")
        self.send_response({
            'info': "Sensitivity info returned",
            "projects": sensitivities
        })

    def list_projects_for_feature(self):
        """Lists all projects containing a specific feature."""

        self.validate_args(self.request.arguments, ['feature_class_id'])

        projects = get_projects_for_feature(
            int(self.get_argument('feature_class_id')), self.proj_paths.USERS_FOLDER)

        self.send_response({
            'info': "Projects info returned",
            "projects": projects
        })
