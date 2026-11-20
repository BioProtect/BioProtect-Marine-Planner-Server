import json
import uuid

from handlers.base_handler import BaseHandler
from psycopg2 import sql
from services.file_service import create_zipfile
from services.service_error import ServicesError, raise_error


class FeatureHandler(BaseHandler):
    """
    REST HTTP handler for feature-related operations, including creation, deletion, import, export,
    fetching, and listing projects for a feature.
    """

    def initialize(self, pg, finish_feature_import):
        super().initialize(pg=pg)
        self.finish_feature_import = finish_feature_import

    async def get(self):
        """
        Handles GET requests for various feature-related actions based on query parameters.
        """
        try:
            action = self.get_argument('action', None)

            if action == 'get':
                await self.get_feature()
            elif action == 'get-all':
                await self.get_all_features()
            elif action == 'delete':
                await self.delete_feature()
            elif action == 'export':
                await self.export_feature()
            elif action == 'list-projects':
                await self.list_projects_for_feature()
            elif action == 'planning-units':
                await self.get_feature_planning_units()
            elif action == 'get-sensitivities':
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

            if action == 'create-from-drawing':
                await self.create_feature_from_drawing()
            else:
                raise ServicesError("Invalid action specified.")

        except ServicesError as e:
            raise_error(self, e.args[0])

    async def get_feature(self):
        """Fetches feature information from PostGIS."""
        self.validate_args(self.request.arguments, ['id'])
        unique_id = self.get_argument("id")

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

    async def get_all_features(self):
        """Fetches all features information from PostGIS."""
        query = (
            """
            SELECT unique_id::integer AS id, feature_class_name, alias, description,
            _area AS area, extent, to_char(creation_date, 'DD/MM/YY HH24:MI:SS') AS creation_date,
            tilesetid, source, created_by
            FROM bioprotect.metadata_interest_features;
            """
        )

        data = await self.pg.execute(query, return_format="DataFrame")
        self.send_response({"info": "all features returned",
                            "data": data.to_dict(orient="records")})

    async def delete_feature(self):
        """Deletes a feature class and its associated metadata record."""
        self.validate_args(self.request.arguments, ['feature'])
        feature_class_name = self.get_argument('feature')

        feature_data = await self.pg.execute(
            """
            SELECT unique_id, created_by FROM bioprotect.metadata_interest_features WHERE feature_class_name = %s;
            """,
            data=[feature_class_name],
            return_format="Dict"
        )

        if not feature_data:
            raise ServicesError("Feature not found.")

        created_by = feature_data[0].get("created_by")
        if created_by == "global admin":
            raise ServicesError(
                "This is a system feature and cannot be deleted.")

        # Same rule the features dialog applies: admins, or the user who
        # created the feature
        user = self.get_current_user()
        role = self.get_secure_cookie("role")
        is_admin = bool(role) and role.decode("utf-8") == "Admin"
        if not is_admin and (not user or created_by != user):
            raise ServicesError("You can only delete features you created.")

        # Deleting cascades into project_features and pu_feature_amounts, so
        # a feature in use would silently vanish from those projects
        projects = await self._projects_using_feature(
            feature_data[0]["unique_id"])
        if projects:
            names = ", ".join(p["name"] for p in projects)
            raise ServicesError(
                f"The feature cannot be deleted as it is used in: {names}")

        await self.pg.execute(sql.SQL("DROP TABLE IF EXISTS bioprotect.{};").format(sql.Identifier(feature_class_name)))
        await self.pg.execute("DELETE FROM bioprotect.metadata_interest_features WHERE feature_class_name = %s;", [feature_class_name])

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

    async def create_feature_from_drawing(self):
        """Creates a feature from polygons drawn on the map, with land cut out.

        Body: {"name", "description", "geojson": FeatureCollection of polygons}.
        """
        data = json.loads(self.request.body or "{}")
        name = (data.get('name') or '').strip()
        polygons = [f.get('geometry') for f in (data.get('geojson') or {}).get('features', [])]
        polygons = [g for g in polygons if g and g.get('type') in ('Polygon', 'MultiPolygon')]
        if not name or not polygons:
            raise ServicesError("A name and at least one drawn polygon are required.")

        has_land = await self.pg.execute(
            "SELECT EXISTS (SELECT 1 FROM bioprotect.land) AS ok", return_format="Dict")
        if not has_land[0]["ok"]:
            raise ServicesError(
                "Land data is not loaded on this server (run db/load_land.sh), so drawn features can't have land removed.")

        feature_class_name = "f_" + uuid.uuid4().hex[:30]
        # ponytail: one ST_Union of the land pieces under the drawing. 0.5 s for a
        # bay, 12 s for 36,000 km2 of Norwegian fjords; subdivide the drawing first
        # if people draw whole coastlines.
        await self.pg.execute(sql.SQL("""
            CREATE TABLE bioprotect.{} AS
            WITH d AS (
                SELECT ST_MakeValid(ST_Union(ST_SetSRID(ST_GeomFromGeoJSON(g), 4326))) AS g
                FROM unnest(%s::text[]) AS g),
            sea AS (
                SELECT ST_CollectionExtract(ST_Difference(d.g, COALESCE(
                    (SELECT ST_Union(l.geom) FROM bioprotect.land l WHERE ST_Intersects(l.geom, d.g)),
                    'SRID=4326;GEOMETRYCOLLECTION EMPTY'::geometry)), 3) AS g
                FROM d)
            SELECT (ST_Dump(sea.g)).geom::geometry(Polygon, 4326) AS geometry FROM sea
        """).format(sql.Identifier(feature_class_name)), [[json.dumps(g) for g in polygons]])

        left = await self.pg.execute(
            sql.SQL("SELECT count(*) AS n FROM bioprotect.{}").format(sql.Identifier(feature_class_name)),
            return_format="Dict")
        if left[0]["n"] == 0:
            await self.pg.execute(sql.SQL("DROP TABLE bioprotect.{}").format(sql.Identifier(feature_class_name)))
            raise ServicesError("The drawn area is entirely on land. Draw over water.")

        user = self.get_current_user()
        feature_id = await self.finish_feature_import(
            feature_class_name, name, data.get('description') or '', "Drawn on screen", user)

        self.send_response({
            'info': f"Feature '{name}' created",
            'id': feature_id,
            'feature_class_name': feature_class_name,
        })

    async def get_feature_planning_units(self):
        """Gets the planning unit h3_index values where a feature occurs."""

        self.validate_args(self.request.arguments, ['project_id', 'id'])
        project_id = int(self.get_argument("project_id"))
        feature_id = int(self.get_argument("id"))

        rows = await self.pg.execute(
            """
            SELECT DISTINCT h3_index
            FROM bioprotect.pu_feature_amounts
            WHERE project_id = %s AND feature_unique_id = %s
            """,
            data=[project_id, feature_id],
            return_format="Array"
        )
        puids = [row["h3_index"] for row in rows] if rows else []

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

    async def _projects_using_feature(self, feature_unique_id):
        """Every project (any user's) that includes the feature."""
        return await self.pg.execute(
            """
            SELECT p.id, p.name
            FROM bioprotect.project_features pf
            JOIN bioprotect.projects p ON p.id = pf.project_id
            WHERE pf.feature_unique_id = %s
            ORDER BY p.name;
            """,
            data=[feature_unique_id],
            return_format="Array"
        )

    async def list_projects_for_feature(self):
        """Lists all projects containing a specific feature."""

        self.validate_args(self.request.arguments, ['feature-id'])
        try:
            feature_id = int(self.get_argument('feature-id'))
        except ValueError:
            raise ServicesError("Invalid feature-id")
        projects = await self._projects_using_feature(feature_id)

        self.send_response({
            'info': "Projects info returned",
            "projects": projects
        })
