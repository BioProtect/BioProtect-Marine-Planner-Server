import asyncio
import uuid
from os import sep
from os.path import join, relpath

import pandas as pd
from classes.db_config import DBConfig
from handlers.base_handler import BaseHandler
from psycopg2 import sql
from services.file_service import (check_zipped_shapefile,
                                   delete_zipped_shapefile, file_to_df,
                                   get_files_in_folder,
                                   get_key_values_from_file,
                                   get_shapefile_fieldnames,
                                   normalize_dataframe, unzip_shapefile)
from services.project_service import set_folder_paths, write_csv
from services.service_error import ServicesError, raise_error
from sqlalchemy import create_engine
from tornado import escape


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

    @staticmethod
    def normalize_name(name):
        return name.lower().replace(" ", "_").replace("-", "_").replace("/", "_")

    @staticmethod
    def get_scale_level(res):
        return "basin" if res <= 6 else "regional" if res == 7 else "local"

    @staticmethod
    def create_sql_engine():
        config = DBConfig()
        db_url = f"postgresql://{config.DATABASE_USER}:{config.DATABASE_PASSWORD}@{config.DATABASE_HOST}/{config.DATABASE_NAME}"
        return create_engine(db_url)

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
            elif action == "get_cost_layer":
                await self.get_pu_costs_layer()
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
                await self.update_planning_units()
            elif action == 'import':
                await self.import_planning_unit_grid()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    # GET FUNCTIONS ###############################################################

    async def delete_planning_unit_grid(self):
        self.validate_args(self.request.arguments, ['planning_grid_name'])
        planning_grid = self.get_argument('planning_grid_name')

        grid_data = await self.pg.execute(
            """
            SELECT created_by, source
            FROM bioprotect.metadata_planning_units
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
            "DELETE FROM bioprotect.metadata_planning_units WHERE feature_class_name = %s;",
            data=[planning_grid]
        )

        await self.pg.execute(
            sql.SQL("DROP TABLE IF EXISTS bioprotect.{};").format(
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
        print("Retrieving planning unit grids....................")
        # bioprotect.get_pu_grids() is a POSTGIS function (functions table in the databse) that retrieves planning unit grids
        planning_unit_grids = await self.pg.execute("SELECT * FROM bioprotect.get_pu_grids();", return_format="Array")

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

    async def get_pu_costs_layer(self):
        """
        Returns Marxan-style cost layer data for project PUs.
        """
        self.validate_args(self.request.arguments, ['user', 'project_id'])
        project_id = self.get_argument("project_id")

        rows = await self.pg.execute(
            """
            SELECT h3_index, cost
            FROM bioprotect.project_pus
            WHERE project_id = %s
            """,
            [project_id],
            return_format="Dict"
        )
        if not rows:
            self.send_response({"data": [], "min": None, "max": None})
            return

        df = pd.DataFrame(rows)
        min_cost, max_cost = float(df["cost"].min()), float(df["cost"].max())

        num_bins = 9
        bins = pd.cut(df["cost"], bins=num_bins, include_lowest=True)

        grouped = [[] for _ in range(num_bins)]
        bin_ranges = [[] for _ in range(num_bins)]
        for h3, cost, bin_interval in zip(df["h3_index"], df["cost"], bins):
            idx = list(bins.cat.categories).index(bin_interval)
            grouped[idx].append(h3)
            if not bin_ranges[idx]:
                bin_ranges[idx] = [
                    float(bin_interval.left), float(bin_interval.right)]

        self.send_response({
            "info": "PU costs layer generated",
            "data": grouped,        # list of PU ids per bin
            "ranges": bin_ranges,   # min/max for each bin
            "min": min_cost,
            "max": max_cost
        })

    # async def get_planning_unit_data(self):
    #     self.validate_args(self.request.arguments, ['user', 'project', 'puid'])
    #     files = self.projectData["files"]
    #     puid = self.get_argument('puid')

    #     pu_df = file_to_df(join(self.input_folder, files["PUNAME"]))
    #     pu_data = pu_df.loc[pu_df['id'] == int(puid)].iloc[0]

    #     df = file_to_df(join(self.input_folder, files["PUVSPRNAME"]))
    #     features = df.loc[df['pu'] == int(
    #         puid)] if not df.empty else pd.DataFrame()

    #     self.send_response({
    #         "info": 'Planning unit data returned',
    #         "data": {
    #             'features': features.to_dict(orient="records"),
    #             'pu_data': pu_data.to_dict()
    #         }
    #     })

    async def get_planning_unit_data(self):
        """
        Returns data for a single planning unit (PU).
        Includes cost, status, and feature amounts in this PU.
        """
        self.validate_args(self.request.arguments, [
                           'user', 'project_id', 'h3_index'])
        project_id = self.get_argument('project_id')
        h3_index = self.get_argument('h3_index')  # h3_index for the PU

        # Fetch the PU record by H3 index
        pu_rows = await self.pg.execute(
            """
            SELECT id, h3_index, cost, status
            FROM bioprotect.project_pus
            WHERE project_id = %s AND h3_index = %s
            """,
            [project_id, h3_index],
            return_format="Dict"
        )
        if not pu_rows:
            raise ServicesError(
                f"Planning unit {h3_index} not found in project {project_id}.")
        pu_data = pu_rows[0]

        # Fetch all feature amounts for this PU
        feature_rows = await self.pg.execute(
            """
            SELECT 
                f.unique_id AS feature_id,
                f.alias AS feature_name,
                COALESCE(pfa.amount, NULL) AS amount
            FROM bioprotect.project_features pf
            JOIN bioprotect.metadata_interest_features f
            ON f.unique_id = pf.feature_unique_id
            LEFT JOIN bioprotect.pu_feature_amounts pfa
            ON pfa.feature_unique_id = pf.feature_unique_id
            AND pfa.project_id = pf.project_id
            AND pfa.h3_index = %s
            WHERE pf.project_id = %s
            ORDER BY f.alias
            """,
            [h3_index, project_id],
            return_format="Dict"
        )

        # Normalize for UI compatibility
        pu_data["cost"] = float(
            pu_data["cost"]) if pu_data["cost"] is not None else 0

        # Send response
        self.send_response({
            "info": "Planning unit data returned",
            "data": {
                "pu_data": pu_data,
                "features": feature_rows or []
            }
        })
    # POST FUNCTIONS ###############################################################

    # async def update_pu_file(self):
    #     args = self.request.arguments
    #     self.validate_args(args, ['user', 'project'])

    #     status1_ids = self.get_int_array_from_arg(args, "status1")
    #     status2_ids = self.get_int_array_from_arg(args, "status2")
    #     status3_ids = self.get_int_array_from_arg(args, "status3")

    #     status1 = self.create_status_dataframe(status1_ids, 1)
    #     status2 = self.create_status_dataframe(status2_ids, 2)
    #     status3 = self.create_status_dataframe(status3_ids, 3)

    #     pu_file_path = join(
    #         self.input_folder, self.projectData["files"]["PUNAME"])
    #     df = file_to_df(pu_file_path)

    #     df['status'] = 0
    #     status_updates = pd.concat([status1, status2, status3])

    #     df = df.merge(status_updates, on='id', how='left')
    #     df['status'] = df['status_new'].fillna(df['status']).astype('int')
    #     df = df.drop(columns=['status_new'])
    #     df = df.astype({'id': 'int64', 'cost': 'int64', 'status': 'int64'})
    #     df = df.sort_values(by='id')

    #     await write_csv(self, "PUNAME", df)

    #     self.send_response({'info': "pu.dat file updated"})

    async def update_planning_units(self):
        """
        Updates planning unit statuses in the project_pus table.
        Expects JSON body like:
        """
        try:
            body = escape.json_decode(self.request.body)
            user = body.get("user")
            project_name = body.get("project")
            status1_ids = body.get("status1", [])
            status2_ids = body.get("status2", [])
            status3_ids = body.get("status3", [])

            if not user or not project_name:
                raise ServicesError(
                    "Missing required fields 'user' or 'project'.")

            # resolve project_id
            project_row = await self.pg.execute(
                "SELECT id FROM bioprotect.projects WHERE name = %s",
                [project_name],
                return_format="Dict"
            )
            if not project_row:
                raise ServicesError(f"Project '{project_name}' not found.")
            project_id = project_row[0]["id"]

            # reset all statuses to 0 for this project
            await self.pg.execute(
                "UPDATE bioprotect.project_pus SET status = 0 WHERE project_id = %s",
                [project_id]
            )

            # apply updates for each status group
            if status1_ids:
                await self.pg.execute(
                    "UPDATE bioprotect.project_pus SET status = 1 WHERE project_id = %s AND h3_index = ANY(%s)",
                    [project_id, status1_ids]
                )
            if status2_ids:
                await self.pg.execute(
                    "UPDATE bioprotect.project_pus SET status = 2 WHERE project_id = %s AND h3_index = ANY(%s)",
                    [project_id, status2_ids]
                )
            if status3_ids:
                await self.pg.execute(
                    "UPDATE bioprotect.project_pus SET status = 3 WHERE project_id = %s AND h3_index = ANY(%s)",
                    [project_id, status3_ids]
                )

            self.send_response({'info': "Planning unit statuses updated"})

        except ServicesError as e:
            raise_error(self, e.args[0])
        except Exception as e:
            raise_error(self, str(e))

            return

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
                INSERT INTO bioprotect.metadata_planning_units(
                    feature_class_name, alias, description, creation_date, source, created_by, tilesetid
                ) VALUES (%s, %s, %s, now(), 'Imported from shapefile', %s, %s);
                """,
                [feature_class_name, name, description, user, tileset_id]
            )

            await self.pg.importShapefile(import_folder, f"{root_filename}.shp", feature_class_name)
            await self.pg.is_valid(feature_class_name)

            await self.pg.execute(
                sql.SQL("ALTER TABLE bioprotect.{} ALTER COLUMN puid TYPE integer;").format(
                    sql.Identifier(feature_class_name))
            )

            await self.pg.execute(
                sql.SQL(
                    """
                    UPDATE bioprotect.metadata_planning_units
                    SET envelope = (
                        SELECT ST_Transform(ST_Envelope(ST_Collect(geometry)), 4326)
                        FROM bioprotect.{}
                    )
                    WHERE feature_class_name = %s;
                    """
                ).format(sql.Identifier(feature_class_name)),
                [feature_class_name]
            )

            await self.pg.execute(
                sql.SQL(
                    """
                    UPDATE bioprotect.metadata_planning_units
                    SET planning_unit_count = (
                        SELECT COUNT(puid)
                        FROM bioprotect.{}
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
