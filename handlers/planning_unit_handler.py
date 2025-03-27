import asyncio
import uuid
from os import sep
from os.path import join, relpath

import pandas as pd
from psycopg2 import sql
from services.file_service import (check_zipped_shapefile,
                                   delete_zipped_shapefile, file_to_df,
                                   get_files_in_folder,
                                   get_key_values_from_file,
                                   get_shapefile_fieldnames,
                                   normalize_dataframe, unzip_shapefile)
from services.project_service import set_folder_paths, write_csv
from services.service_error import ServicesError, raise_error
from handlers.base_handler import BaseHandler
from services.queries import get_pu_grids_query


class PlanningUnitHandler(BaseHandler):
    """
    Handles planning unit-related operations, such as importing, exporting, deleting,
    retrieving, and updating planning units.
    """

    def initialize(self, pg, upload_tileset):
        super().initialize()
        self.pg = pg
        self.upload_tileset = upload_tileset

    @staticmethod
    def create_status_dataframe(puid_array, pu_status):
        return pd.DataFrame({
            'id': [int(puid) for puid in puid_array],
            'status_new': [pu_status] * len(puid_array)
        }, dtype='int64')

    @staticmethod
    def get_int_array_from_arg(arguments, arg_name):
        return [
            int(s) for s in arguments.get(arg_name, [b""])[0].decode("utf-8").split(",")
        ] if arg_name in arguments else []

    def get_projects_for_planning_grid(self, feature_class_name):
        user_folder = self.proj_paths.USERS_FOLDER
        input_dat_files = get_files_in_folder(user_folder, "input.dat")

        projects = [
            {'user': relpath(file_path, user_folder).split(sep)[0],
             'name': relpath(file_path, user_folder).split(sep)[1]}
            for file_path in input_dat_files
            if get_key_values_from_file(file_path).get('PLANNING_UNIT_NAME') == feature_class_name
        ]

        return projects

    async def get(self):
        """
        Handles GET requests for planning unit-related actions.
        """
        action = self.get_argument('action', None)

        try:
            if action == 'delete':
                await self.delete_planning_unit_grid()
            elif action == 'export':
                await self.export_planning_unit_grid()
            elif action == 'list':
                await self.get_planning_unit_grids()
            elif action == 'projects':
                self.list_projects_for_planning_grid()
            elif action == 'cost_data':
                await self.get_planning_units_cost_data()
            elif action == 'data':
                await self.get_planning_unit_data()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def post(self):
        """
        Handles POST requests for planning unit updates.
        """
        try:
            action = self.get_argument('action', None)
            if action == 'update':
                await self.update_pu_file()
            elif action == 'import':
                await self.import_planning_unit_grid()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def delete_planning_unit_grid(self):
        self.validate_args(self.request.arguments, ['planning_grid_name'])
        planning_grid = self.get_argument('planning_grid_name')

        grid_data = await self.pg.execute(
            """
            SELECT created_by, source
            FROM marxan.metadata_planning_units
            WHERE feature_class_name = %s;
            """,
            data=[planning_grid],
            return_format="Dict"
        )

        if not grid_data:
            return

        created_by = grid_data[0].get("created_by")
        if created_by == "global admin":
            raise ServicesError(
                "The planning grid cannot be deleted as it is a system-supplied item.")

        projects = self.get_projects_for_planning_grid(planning_grid)
        if projects:
            raise ServicesError(
                "Grid cannot be deleted as it is used in one or more projects.")

        source = grid_data[0].get("source")
        if source != "planning_grid function":
            del_tileset(planning_grid)

        await self.pg.execute(
            "DELETE FROM marxan.metadata_planning_units WHERE feature_class_name = %s;",
            data=[planning_grid]
        )

        await self.pg.execute(
            sql.SQL("DROP TABLE IF EXISTS marxan.{};").format(
                sql.Identifier(planning_grid))
        )

        self.send_response({'info': 'Planning grid deleted'})

    async def export_planning_unit_grid(self):
        self.validate_args(self.request.arguments, ['name'])
        feature_class_name = self.get_argument('name')
        folder = self.proj_paths.EXPORT_FOLDER

        await self.pg.exportToShapefile(folder, feature_class_name, tEpsgCode="EPSG:4326")
        zipfilename = create_zipfile(folder, feature_class_name)

        self.send_response({
            'info': f"Planning grid '{feature_class_name}' exported",
            'filename': f"{zipfilename}.zip"
        })

    async def get_planning_unit_grids(self):
        planning_unit_grids = await self.pg.execute(get_pu_grids_query, return_format="Dict")
        self.send_response({
            'info': 'Planning unit grids retrieved',
            'planning_unit_grids': planning_unit_grids
        })

    def list_projects_for_planning_grid(self):
        self.validate_args(self.request.arguments, ['feature_class_name'])
        projects = self.get_projects_for_planning_grid(
            self.get_argument('feature_class_name'))
        self.send_response({
            'info': "Projects info returned",
            'projects': projects
        })

    async def get_planning_units_cost_data(self):
        self.validate_args(self.request.arguments, ['user', 'project'])
        set_folder_paths(self, self.request.arguments,
                         self.proj_paths.USERS_FOLDER)

        df = file_to_df(join(
            self.input_folder, self.projectData["files"]["PUNAME"]))
        data = normalize_dataframe(df, "cost", "id", 9)

        self.send_response({
            "data": data[0],
            'min': str(data[1]),
            'max': str(data[2])
        })

    async def get_planning_unit_data(self):
        self.validate_args(self.request.arguments, ['user', 'project', 'puid'])
        files = self.projectData["files"]
        puid = self.get_argument('puid')

        pu_df = file_to_df(join(self.input_folder, files["PUNAME"]))
        pu_data = pu_df.loc[pu_df['id'] == int(puid)].iloc[0]

        df = file_to_df(join(self.input_folder, files["PUVSPRNAME"]))
        features = df.loc[df['pu'] == int(
            puid)] if not df.empty else pd.DataFrame()

        self.send_response({
            "info": 'Planning unit data returned',
            "data": {
                'features': features.to_dict(orient="records"),
                'pu_data': pu_data.to_dict()
            }
        })

    async def update_pu_file(self):
        args = self.request.arguments
        self.validate_args(args, ['user', 'project'])

        status1_ids = self.get_int_array_from_arg(args, "status1")
        status2_ids = self.get_int_array_from_arg(args, "status2")
        status3_ids = self.get_int_array_from_arg(args, "status3")

        status1 = self.create_status_dataframe(status1_ids, 1)
        status2 = self.create_status_dataframe(status2_ids, 2)
        status3 = self.create_status_dataframe(status3_ids, 3)

        pu_file_path = join(
            self.input_folder, self.projectData["files"]["PUNAME"])
        df = file_to_df(pu_file_path)

        df['status'] = 0
        status_updates = pd.concat([status1, status2, status3])

        df = df.merge(status_updates, on='id', how='left')
        df['status'] = df['status_new'].fillna(df['status']).astype('int')
        df = df.drop(columns=['status_new'])
        df = df.astype({'id': 'int64', 'cost': 'int64', 'status': 'int64'})
        df = df.sort_values(by='id')

        await write_csv(self, "PUNAME", df)

        self.send_response({'info': "pu.dat file updated"})

    async def import_planning_unit_grid(self):
        self.validate_args(self.request.arguments, [
                           'filename', 'name', 'description'])
        filename = self.get_argument('filename')
        name = self.get_argument('name')
        description = self.get_argument('description')
        user = self.get_current_user()
        import_folder = self.proj_paths.IMPORT_FOLDER

        root_filename = await asyncio.get_running_loop().run_in_executor(
            None, unzip_shapefile, import_folder, filename
        )

        feature_class_name = "pu_" + uuid.uuid4().hex[:29]
        tileset_id = f"{MAPBOX_USER}.{feature_class_name}"
        shapefile_path = join(
            import_folder, f"{root_filename}.shp")

        try:
            check_zipped_shapefile(shapefile_path)
            fieldnames = get_shapefile_fieldnames(shapefile_path)
            if "PUID" in fieldnames:
                raise ServicesError(
                    "The field 'puid' in the shapefile must be lowercase.")

            await self.pg.execute(
                """
                INSERT INTO marxan.metadata_planning_units(
                    feature_class_name, alias, description, creation_date, source, created_by, tilesetid
                ) VALUES (%s, %s, %s, now(), 'Imported from shapefile', %s, %s);
                """,
                [feature_class_name, name, description, user, tileset_id]
            )

            await self.pg.importShapefile(import_folder, f"{root_filename}.shp", feature_class_name)
            await self.pg.isValid(feature_class_name)

            await self.pg.execute(
                sql.SQL("ALTER TABLE marxan.{} ALTER COLUMN puid TYPE integer;").format(
                    sql.Identifier(feature_class_name))
            )

            await self.pg.execute(
                sql.SQL(
                    """
                    UPDATE marxan.metadata_planning_units
                    SET envelope = (
                        SELECT ST_Transform(ST_Envelope(ST_Collect(geometry)), 4326)
                        FROM marxan.{}
                    )
                    WHERE feature_class_name = %s;
                    """
                ).format(sql.Identifier(feature_class_name)),
                [feature_class_name]
            )

            await self.pg.execute(
                sql.SQL(
                    """
                    UPDATE marxan.metadata_planning_units
                    SET planning_unit_count = (
                        SELECT COUNT(puid)
                        FROM marxan.{}
                    )
                    WHERE feature_class_name = %s;
                    """
                ).format(sql.Identifier(feature_class_name)),
                [feature_class_name]
            )

            upload_id = self.upload_tileset(shapefile_path, feature_class_name)

        except ServicesError as e:
            raise
        finally:
            await asyncio.get_running_loop().run_in_executor(
                None, delete_zipped_shapefile, import_folder, filename, root_filename
            )

        self.send_response({
            'info': f"Planning grid '{name}' imported",
            'feature_class_name': feature_class_name,
            'uploadId': upload_id,
            'alias': name
        })
