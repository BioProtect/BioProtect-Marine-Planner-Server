import json

import geopandas as gpd
import h3
from classes.db_config import DBConfig
from handlers.websocket_handler import SocketHandler
from psycopg2 import sql
from services.martin_service import restart_martin
from services.service_error import ServicesError
from shapely.geometry import Polygon
from sqlalchemy import create_engine, text


class PlanningGridWebSocketHandler(SocketHandler):
    def initialize(self, pg):
        super().initialize()
        self.pg = pg

    def check_origin(self, origin):
        return True  # for development purposes

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

    async def open(self):
        try:
            await super().open({'info': "Processing planning grid.."})
        except ServicesError:
            pass

    async def on_message(self, message):
        try:
            # Parse incoming message
            data = json.loads(message)
            print('data: ', data)

            shapefile_path = data.get('shapefile_path')
            print('shapefile_path: ', shapefile_path)
            alias = data.get('alias')
            description = data.get('description')
            resolution = int(data.get('resolution', 7))  # fallback if missing
            created_by = self.current_user

            if not all([shapefile_path, alias, description]):
                raise ServicesError(
                    "Missing required fields in WebSocket message.")

            scale_level = self.get_scale_level(resolution)
            view_name = f"v_h3_{self.normalize_name(alias)}_res{resolution}"
            project_area_name = alias

            self.send_response({'info': "📦 Reading shapefile..."})
            df = gpd.read_file(shapefile_path)

            unique_cells = set()
            for _, row in df.iterrows():
                self.send_response(
                    {'info': "📦 looping through shapefile rows..."})
                geom = row["geometry"]
                parts = [geom] if geom.geom_type == "Polygon" else list(
                    geom.geoms)
                for part in parts:
                    self.send_response({'info': "📦 parsing geometries..."})
                    unique_cells |= set(h3.geo_to_cells(
                        part.__geo_interface__, resolution))

            records = []
            self.send_response({'info': "📦 Building records for db..."})
            for h in unique_cells:

                coords = [(lon, lat)
                          for lat, lon in h3.cell_to_boundary(h)]
                records.append({
                    "h3_index": h,
                    "resolution": resolution,
                    "scale_level": scale_level,
                    "project_area": project_area_name,
                    "geometry": Polygon(coords),
                })

            self.send_response(
                {'info': f"🧱 Generated {len(records)} H3 records"})
            # self.send_response({'status': 'Preprocessing', 'info': "Checking the geometry.."})

            gdf_out = gpd.GeoDataFrame(
                records, geometry="geometry", crs="EPSG:4326")
            engine = self.create_sql_engine()

            self.send_response({'info': "📤 Writing H3 cells to database..."})
            await self.pg.execute(
                """
                DELETE FROM bioprotect.h3_cells
                WHERE project_area = %s AND resolution = %s
                """,
                [project_area_name, resolution]
            )
            gdf_out.to_postgis(
                "h3_cells", engine, schema="bioprotect", if_exists="append", index=False)

            self.send_response({'info': "📐 Creating PostGIS view..."})
            existing_links = await self.pg.execute("""
                SELECT COUNT(*) AS count
                FROM bioprotect.projects p
                JOIN bioprotect.metadata_planning_units mpu
                ON p.planning_unit_id = mpu.unique_id
                WHERE mpu.feature_class_name = %s;
            """, [view_name], return_format="Dict")

            if existing_links[0]["count"] > 0:
                raise ServicesError(
                    f"The planning grid '{view_name}' is linked to existing projects and cannot be modified.")

            # await self.pg.execute(text(f"DROP VIEW IF EXISTS bioprotect.{view_name} CASCADE"))
            await self.pg.execute(sql.SQL("""
                CREATE VIEW bioprotect.{} AS
                SELECT h3_index, resolution, scale_level, project_area, geometry
                FROM bioprotect.h3_cells
                WHERE project_area = %s AND resolution = %s;
            """).format(sql.Identifier(view_name)), data=[project_area_name, resolution])

            self.send_response(
                {'info': "📝 Inserting planning unit metadata..."})
            await self.pg.execute(sql.SQL("""
                INSERT INTO bioprotect.metadata_planning_units (
                    feature_class_name, alias, description, domain, _area,
                    envelope, creation_date, source, created_by, tilesetid, planning_unit_count
                )
                SELECT
                    %s, %s, %s, 'marine', 0,
                    ST_Multi(ST_Envelope(ST_Collect(geometry))),
                    NOW(), 'h3_cells', %s, %s, COUNT(*)
                FROM bioprotect.{}
                ON CONFLICT (feature_class_name) DO NOTHING;
            """).format(sql.Identifier(view_name)), data=[view_name, alias, description, created_by, view_name])

            result = await self.pg.execute(
                "SELECT unique_id FROM bioprotect.metadata_planning_units WHERE feature_class_name = %s",
                data=[view_name],
                return_format="Array"
            )
            planning_unit_id = result[0]["unique_id"] if result else None

            self.send_response({'info': "🔄 Reloading Martin..."})
            restart_martin()

            self.send_response({
                "success": True,
                "info": f"✅ Planning grid '{alias}' created successfully.",
                "view_name": view_name,
                "planning_unit_id": planning_unit_id
            })
        except Exception as e:
            self.send_response({
                "error": f"❌ Failed to create planning grid: {str(e)}"
            })
            # You can optionally clean up here
            await self.pg.execute(
                "DELETE FROM bioprotect.metadata_planning_units WHERE feature_class_name = %s",
                [view_name]
            )
            await self.pg.execute(
                sql.SQL("DROP VIEW IF EXISTS bioprotect.{} CASCADE").format(
                    sql.Identifier(view_name))
            )
