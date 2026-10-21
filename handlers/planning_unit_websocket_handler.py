import asyncio
import json
from functools import partial

import geopandas as gpd
import h3
from classes.db_config import DBConfig
from handlers.websocket_handler import SocketHandler
from psycopg2 import sql
from services.service_error import ServicesError
from shapely.geometry import Polygon
from sqlalchemy import create_engine, text


MAX_DRAWN_CELLS = 1_000_000

# ponytail: grids crossing 180° are rejected, not supported. A hex built from
# cell_to_boundary across it spans the globe, and drawn longitudes past 180
# polyfill to half the cells. The fix is h3_postgis's
# h3_cell_to_boundary_geometry (splits at 180°), but it needs h3_cells.geometry
# widened from Polygon, which means dropping and rebuilding every grid view.
# Do it when a Pacific project needs it.
ANTIMERIDIAN_ERROR = (
    "Planning grids that cross the 180° meridian aren't supported yet. "
    "Keep the area to one side of it.")


class PlanningGridWSHandler(SocketHandler):
    def initialize(self, pg):
        super().initialize(pg=pg)

    def check_origin(self, origin):
        return True  # for development purposes

    @staticmethod
    def normalize_name(name):
        return name.lower().replace(" ", "_").replace("-", "_").replace("/", "_")

    @staticmethod
    def get_scale_level(res):
        # return "basin" if res <= 6 else "regional" if res == 7 else "local"
        return (
            "basin" if res <= 6
            else "regional" if res == 7
            else "local"
        )

    @staticmethod
    def create_sql_engine():
        config = DBConfig()
        db_url = f"postgresql://{config.DATABASE_USER}:{config.DATABASE_PASSWORD}@{config.DATABASE_HOST}/{config.DATABASE_NAME}"
        return create_engine(db_url)

    @staticmethod
    def shapes_to_cells(geometries, resolution):
        """H3 cells covering every polygon in the shapefile."""
        cells = set()
        for geom in geometries:
            parts = [geom] if geom.geom_type == "Polygon" else list(
                geom.geoms)
            for part in parts:
                cells |= set(h3.geo_to_cells(
                    part.__geo_interface__, resolution))
        return cells

    @staticmethod
    def cells_to_gdf(cells, resolution, scale_level, project_area_name):
        """One h3_cells row per cell, with its hexagon geometry."""
        records = []
        for h in cells:
            ring = [(lon, lat) for lat, lon in h3.cell_to_boundary(h)]
            lons = [lon for lon, _ in ring]
            if max(lons) - min(lons) > 180:  # straddles 180°, see ANTIMERIDIAN_ERROR
                raise ServicesError(ANTIMERIDIAN_ERROR)
            records.append({
                "h3_index": h,
                "resolution": resolution,
                "scale_level": scale_level,
                "project_area": project_area_name,
                "geometry": Polygon(ring),
            })
        return gpd.GeoDataFrame(records, geometry="geometry", crs="EPSG:4326")

    async def open(self):
        try:
            await super().open({'info': "Processing planning grid.."})
        except ServicesError:
            pass

    async def on_message(self, message):
        """
        Docstring for on_message

        :param self: Description
        :param message: data: {
            "shapefile_path": "/path/to/shapefile.shp",
            OR "geojson": {FeatureCollection drawn on the map} (land is removed),
            "alias": "Case Study Extents Water Only",
            "description": "Description of the grid",
            "resolution": 7}

        Alias is used in bioprotect.h3_cells. 
        Alias in the metadata tabel has the resolution attached (Res 7) to make it more user friendly
        """
        # Parse incoming message
        # get all the necessary data

        data = json.loads(message)
        print('data: ', data)
        created_by = self.current_user
        shapefile_path = data.get('shapefile_path')
        geojson = data.get('geojson')
        project_area_name = data.get('alias')
        description = data.get('description')
        resolution = int(data.get('resolution', 7))
        if resolution not in (6, 7, 8, 9):
            raise ServicesError(
                "Invalid resolution. Allowed values: 6, 7, 8, 9.")

        if not all([shapefile_path or geojson, project_area_name, description]):
            raise ServicesError(
                "Missing required fields in WebSocket message.")

        scale_level = self.get_scale_level(resolution)
        view_name = f"v_h3_{self.normalize_name(project_area_name)}_res{resolution}"
        alias = f"{project_area_name} (Res {resolution})"

        try:
            # Start the processing steps``
            # Check if it already exists
            existing = await self.pg.execute(
                "SELECT 1 FROM bioprotect.metadata_planning_units WHERE feature_class_name = %s",
                [view_name],
                return_format="Dict"
            )
            if existing and geojson:
                # Send and return rather than raise: the except block below
                # would delete the existing grid's metadata and view.
                self.send_response({
                    "error": f"❌ A planning grid named '{project_area_name}' at resolution {resolution} already exists. Choose another name."
                })
                return
            if existing:
                result = await self.pg.execute(
                    """
                    SELECT unique_id 
                    FROM bioprotect.metadata_planning_units 
                    WHERE feature_class_name = %s
                    """,
                    [view_name],
                    return_format="Dict"
                )

                planning_unit_id = result[0]["unique_id"]

                self.send_response({
                    "type": "grid_exists",
                    "info": f"Planning grid '{view_name}' already exists. Using existing grid.",
                    "view_name": view_name,
                    "planning_unit_id": planning_unit_id
                })
                return

            # The blocking steps run in a thread: a message only reaches the
            # client once this coroutine yields, and blocking here would
            # freeze the server for every other user as well.
            loop = asyncio.get_running_loop()

            if geojson:
                has_land = await self.pg.execute(
                    "SELECT EXISTS (SELECT 1 FROM bioprotect.land) AS ok",
                    return_format="Dict")
                if not has_land[0]["ok"]:
                    raise ServicesError(
                        "Land data is not loaded on this server (run db/load_land.sh), so drawn grids can't have land removed.")
                df = gpd.GeoDataFrame.from_features(
                    geojson["features"], crs="EPSG:4326")
                if df.empty or not df.geom_type.isin(["Polygon", "MultiPolygon"]).all():
                    raise ServicesError("Draw at least one polygon.")
                # the map gives unwrapped longitudes when drawing across 180°
                minx, _, maxx, _ = df.total_bounds
                if minx < -180 or maxx > 180:
                    raise ServicesError(ANTIMERIDIAN_ERROR)
                # EPSG:6933 is equal-area, so .area is in m²
                estimate = df.to_crs(6933).area.sum() / 1e6 / \
                    h3.average_hexagon_area(resolution, "km^2")
                if estimate > MAX_DRAWN_CELLS:
                    raise ServicesError(
                        f"The drawn area would make about {estimate:,.0f} planning units "
                        f"(limit {MAX_DRAWN_CELLS:,}). Draw a smaller area or use a coarser resolution.")
            else:
                self.send_response({'info': "📦 Reading shapefile..."})
                df = await loop.run_in_executor(None, gpd.read_file, shapefile_path)

            self.send_response(
                {'info': f"🔷 Converting {len(df):,} shapes to H3 cells..."})
            unique_cells = await loop.run_in_executor(
                None, self.shapes_to_cells, df.geometry, resolution)

            self.send_response(
                {'info': f"🧱 Building {len(unique_cells):,} planning units..."})
            gdf_out = await loop.run_in_executor(
                None, self.cells_to_gdf, unique_cells, resolution,
                scale_level, project_area_name)
            engine = self.create_sql_engine()

            self.send_response(
                {'info': f"📤 Writing {len(gdf_out):,} planning units to the database..."})
            await self.pg.execute(
                """
                DELETE FROM bioprotect.h3_cells
                WHERE project_area = %s AND resolution = %s
                """,
                [project_area_name, resolution]
            )

            await loop.run_in_executor(None, partial(
                gdf_out.to_postgis, "h3_cells", engine,
                schema="bioprotect", if_exists="append", index=False))

            if geojson:
                self.send_response({'info': "🌊 Removing land cells..."})
                removed = await self.pg.execute("""
                    WITH d AS (
                        DELETE FROM bioprotect.h3_cells c
                        WHERE project_area = %s AND resolution = %s
                        AND EXISTS (
                            SELECT 1 FROM bioprotect.land l
                            WHERE ST_Intersects(l.geom, ST_Centroid(c.geometry)))
                        RETURNING 1)
                    SELECT COUNT(*) AS count FROM d
                """, [project_area_name, resolution], return_format="Dict")
                kept = len(gdf_out) - removed[0]["count"]
                if kept == 0:
                    raise ServicesError(
                        "The drawn area is entirely on land. Draw over water.")
                self.send_response(
                    {'info': f"🌊 Removed {removed[0]['count']:,} land cells, {kept:,} remain."})

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
                CREATE MATERIALIZED VIEW bioprotect.{} AS
                SELECT 
                    h3_index,
                    resolution,
                    scale_level,
                    project_area,
                    ST_SetSRID(geometry, 4326)::geometry(Polygon, 4326) AS geometry
                FROM bioprotect.h3_cells
                WHERE project_area = %s AND resolution = %s;
                """).format(sql.Identifier(view_name)), data=[project_area_name, resolution])

            await self.pg.execute(sql.SQL("""
                CREATE INDEX {} ON bioprotect.{}
                USING GIST (geometry);
                """).format(sql.Identifier(f"{view_name}_geom_idx"), sql.Identifier(view_name)))

            # Analyze so planner + Martin see stats
            await self.pg.execute(sql.SQL("""ANALYZE bioprotect.{};""").format(sql.Identifier(view_name)))

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
                sql.SQL("DROP MATERIALIZED VIEW IF EXISTS bioprotect.{} CASCADE").format(
                    sql.Identifier(view_name))
            )
