import asyncio
import json
from os.path import commonpath, realpath

import geopandas as gpd
import pandas as pd
from classes.db_config import DBConfig
from classes.folder_path_config import get_folder_path_config
from handlers.base_handler import BaseHandler
from handlers.planning_unit_websocket_handler import area_km2
from psycopg2 import sql
from services.file_service import create_zipfile
from services.service_error import ServicesError, raise_error
from sqlalchemy import create_engine


class PlanningUnitHandler(BaseHandler):
    """
    Handles planning unit-related operations, such as importing, exporting, deleting,
    retrieving, and updating planning units.
    """

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

    async def get_projects_for_planning_grid(self, feature_class_name):
        """Every project (any user's) built on the given planning grid."""
        return await self.pg.execute(
            """
            SELECT p.id, p.name
            FROM bioprotect.projects p
            JOIN bioprotect.metadata_planning_units mpu
              ON p.planning_unit_id = mpu.unique_id
            WHERE mpu.feature_class_name = %s
            ORDER BY p.name;
            """,
            data=[feature_class_name],
            return_format="Array"
        )

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
                await self.list_projects_for_planning_grid()
            elif action == "get-cost-layer":
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
            elif action == 'estimate':
                await self.estimate_area()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def estimate_area(self):
        """Area of an uploaded shapefile or a drawn FeatureCollection, so the
        new-grid dialogs can suggest a resolution. JSON body: shapefile_path or geojson."""
        data = json.loads(self.request.body or "{}")
        if data.get("geojson"):
            df = gpd.GeoDataFrame.from_features(
                data["geojson"]["features"], crs="EPSG:4326")
        elif data.get("shapefile_path"):
            path = realpath(data["shapefile_path"])
            root = realpath(get_folder_path_config().PROJECT_FOLDER)
            if commonpath([path, root]) != root:
                raise ServicesError("Invalid shapefile path.")
            df = await asyncio.get_running_loop().run_in_executor(
                None, gpd.read_file, path)
        else:
            raise ServicesError("Send shapefile_path or geojson.")
        self.send_response({"area_km2": area_km2(df)})

    # GET FUNCTIONS ###############################################################

    async def delete_planning_unit_grid(self):
        self.validate_args(self.request.arguments, ['planning_grid_name'])
        planning_grid = self.get_argument('planning_grid_name')

        grid_data = await self.pg.execute(
            """
            SELECT unique_id, created_by
            FROM bioprotect.metadata_planning_units
            WHERE feature_class_name = %s;
            """,
            data=[planning_grid],
            return_format="Array"
        )

        if not grid_data:
            raise ServicesError("Planning grid not found.")
        grid_id = grid_data[0]["unique_id"]

        # Only the user who created a grid may delete it. System-supplied
        # grids were created by no real user, so nobody can delete those.
        user = self.get_current_user()
        if not user or grid_data[0]["created_by"] != user:
            raise ServicesError(
                "Only the user who created a planning grid can delete it.")

        projects = await self.get_projects_for_planning_grid(planning_grid)
        if projects:
            names = ", ".join(p["name"] for p in projects)
            raise ServicesError(
                f"The planning grid cannot be deleted as it is used in: {names}")

        # One call, so it is one transaction: the grid's own cells and
        # boundary edges go with it, then the registration and the view.
        await self.pg.execute(
            sql.SQL("""
                DELETE FROM bioprotect.h3_cells hc
                USING bioprotect.{view} v
                WHERE hc.h3_index = v.h3_index
                  AND hc.project_area = v.project_area
                  AND hc.resolution = v.resolution;
                DELETE FROM bioprotect.grid_boundary_edges WHERE planning_unit_id = %(id)s;
                DELETE FROM bioprotect.pu_h3 WHERE pu_id = %(id)s;
                DELETE FROM bioprotect.metadata_planning_units WHERE unique_id = %(id)s;
                DROP MATERIALIZED VIEW bioprotect.{view};
            """).format(view=sql.Identifier(planning_grid)),
            data={"id": grid_id}
        )

        self.send_response({'info': 'Planning grid deleted'})

    async def export_planning_unit_grid(self):
        self.validate_args(self.request.arguments, ['name'])
        feature_class_name = self.get_argument('name')

        # The name ends up in an ogr2ogr command, so only accept a registered grid
        known = await self.pg.execute(
            "SELECT 1 FROM bioprotect.metadata_planning_units WHERE feature_class_name = %s",
            data=[feature_class_name],
            return_format="Array"
        )
        if not known:
            raise ServicesError("Planning grid not found.")

        folder = self.proj_paths.EXPORT_FOLDER
        await self.pg.export_to_shapefile(folder, feature_class_name)
        create_zipfile(folder, feature_class_name)

        # served from /server/exports/
        self.send_response({
            'info': f"Planning grid '{feature_class_name}' exported",
            'filename': f"{feature_class_name}.zip"
        })

    async def get_planning_unit_grids(self):
        print("Retrieving planning unit grids....................")
        # bioprotect.get_pu_grids() is a POSTGIS function (functions table in the databse) that retrieves planning unit grids
        planning_unit_grids = await self.pg.execute("SELECT * FROM bioprotect.get_pu_grids();", return_format="Array")

        self.send_response({
            'info': 'Planning unit grids retrieved',
            'planning_unit_grids': planning_unit_grids
        })

    async def list_projects_for_planning_grid(self):
        self.validate_args(self.request.arguments, ['feature_class_name'])
        projects = await self.get_projects_for_planning_grid(
            self.get_argument('feature_class_name'))
        self.send_response({
            'info': "Projects info returned",
            'projects': projects
        })

    async def get_pu_costs_layer(self):
        self.validate_args(self.request.arguments, ['user', 'project_id'])
        project_id = int(self.get_argument("project_id"))
        resolution = int(self.get_argument("resolution", 7))

        rows = await self.pg.execute(
            """
            SELECT h3_index, cost
            FROM bioprotect.get_planning_units_for_project(%s)
            WHERE cost IS NOT NULL
            """,
            [project_id],
            return_format="Dict"
        )

        if not rows:
            self.send_response(
                {"data": [], "ranges": [], "min": None, "max": None})
            return

        df = pd.DataFrame(rows)
        df["cost"] = df["cost"].astype(float)

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

    async def get_planning_unit_data(self):
        """
        Returns data for a single planning unit (PU).
        Includes cost, status, and feature amounts in this PU.
        """
        self.validate_args(self.request.arguments, [
                           'project_id', 'h3_index'])
        project_id = self.get_argument('project_id')
        h3_index = self.get_argument('h3_index')
        resolution = int(self.get_argument("resolution", 7))

        # Fetch the PU record by H3 index
        pu_rows = await self.pg.execute(
            """
            SELECT pu.h3_index, pu.cost, pu.status,
                   bioprotect.area_km2(
                       hc.geometry, bioprotect.project_area_srid(%s)) AS area_km2
            FROM bioprotect.get_planning_units_for_project(%s) pu
            LEFT JOIN bioprotect.h3_cells hc ON hc.h3_index = pu.h3_index
            WHERE pu.h3_index = %s
            LIMIT 1
            """,
            [project_id, project_id, h3_index],
            return_format="Dict"
        )

        if not pu_rows:
            raise ServicesError(
                f"Planning unit {h3_index} not found in project {project_id}.")
        pu_data = pu_rows[0]
        pu_data["cost"] = (
            float(pu_data["cost"])
            if pu_data["cost"] is not None
            else None
        )
        pu_data["area_km2"] = (
            float(pu_data["area_km2"])
            if pu_data.get("area_km2") is not None
            else None
        )

        # Fetch feature amounts for this PU — only features that are actually
        # present in the hex (amount > 0).  The previous LEFT JOIN returned
        # every project feature with amount = NULL for missing ones.
        feature_rows = await self.pg.execute(
            """
            SELECT
                f.unique_id AS feature_id,
                f.alias     AS feature_name,
                pfa.amount
            FROM bioprotect.pu_feature_amounts pfa
            JOIN bioprotect.metadata_interest_features f
              ON f.unique_id = pfa.feature_unique_id
            WHERE pfa.project_id = %s
              AND pfa.h3_index   = %s
              AND pfa.amount     > 0
            ORDER BY f.alias
            """,
            [project_id, h3_index],
            return_format="Dict"
        )

        # Send response
        self.send_response({
            "info": "Planning unit data returned",
            "data": {
                "pu_data": pu_data,
                "features": feature_rows or []
            }
        })
    # POST FUNCTIONS ###############################################################

    async def update_planning_units(self):
        """
        Updates planning unit statuses for the project's ACTIVE cost profile.
        Expects form fields:
        - user
        - project_id (prefer this) OR project (name)
        - status0/status1/status2... each containing comma-separated h3 indexes
        """
        try:
            self.validate_args(self.request.arguments, ['project_id'])
            # resolve project_id
            project_id = int(self.get_argument("project_id", None))

            if not project_id:
                raise ServicesError(f"Project id not found.")

            def parse_h3_list(arg_name):
                raw = self.get_argument(arg_name, "")
                return [s.strip() for s in raw.split(",") if s.strip()]

            status1 = parse_h3_list("status1")
            print('status1: ', status1)
            status2 = parse_h3_list("status2")
            print('status2: ', status2)

            await self.pg.execute(
                "SELECT bioprotect.set_active_profile_pu_statuses(%s, %s, %s)",
                [project_id, status1, status2]
            )

            self.send_response(
                {"info": "Planning unit statuses updated", "project_id": project_id})

        except ServicesError as e:
            raise_error(self, e.args[0])
        except Exception as e:
            raise_error(self, str(e))
