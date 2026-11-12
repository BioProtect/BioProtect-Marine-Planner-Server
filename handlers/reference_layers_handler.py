"""View-only map layers for the "Reference layers" dialog.

GET  /server/reference-layers
     -> {"wdpa": {"loaded": bool, "release": str|None},
         "layers": [{id, name, description, geom_type, colour, created_by, created_at}]}
POST /server/reference-layers  {"action": "delete", "id": N}   (uploader or Admin)
WS   /server/importReferenceLayer?zipfile=<uploaded zip>&name=..&description=..

Tiles come from Martin (wdpa_tiles, reference_tiles); nothing in the analysis
reads these layers. User layers are visible to everyone (see migration 0020).
"""

import json
import os

from psycopg2 import sql

from classes.folder_path_config import get_folder_path_config
from handlers.base_handler import BaseHandler
from handlers.websocket_handler import SocketHandler
from services.file_service import delete_zipped_shapefile, unzip_shapefile
from services.service_error import ServicesError, raise_error

# Distinct from the WDPA green and the grid/cost colours; picked by layer id.
PALETTE = ["#e6550d", "#7b3294", "#2166ac", "#c51b7d", "#8c510a",
           "#d6604d", "#4d4d4d", "#b8860b"]

# Vertices per subdivide batch: keeps each query well under the pool's 120 s
# cap on prod (~3x slower than dev). Same scale as PIECE_BATCH_POINTS.
BATCH_POINTS = 200_000

GEOM_TYPES = {
    "ST_Polygon": "polygon", "ST_MultiPolygon": "polygon",
    "ST_LineString": "line", "ST_MultiLineString": "line",
    "ST_Point": "point", "ST_MultiPoint": "point",
}


async def _layer_rows(pg):
    return await pg.execute(
        """
        SELECT id, name, description, geom_type, colour, created_by,
               to_char(created_at, 'YYYY-MM-DD') AS created_at
          FROM bioprotect.reference_layers ORDER BY lower(name)
        """, return_format="Array")


class ReferenceLayersHandler(BaseHandler):

    def initialize(self, pg):
        super().initialize(pg=pg)

    async def get(self):
        try:
            await self._get_authenticated_user_id()
            rows = await self.pg.execute(
                """
                SELECT (SELECT release FROM bioprotect.wdpa_release LIMIT 1) AS release,
                       EXISTS (SELECT 1 FROM bioprotect.wdpa_areas) AS loaded
                """,
                return_format="Dict")
            self.send_response({"wdpa": rows[0], "layers": await _layer_rows(self.pg)})
        except ServicesError as e:
            raise_error(self, e.args[0])

    async def post(self):
        try:
            await self._get_authenticated_user_id()
            body = json.loads(self.request.body or b"{}")
            if body.get("action") != "delete":
                raise ServicesError("Unknown action")
            layer_id = int(body["id"])
            rows = await self.pg.execute(
                "SELECT created_by FROM bioprotect.reference_layers WHERE id = %s",
                [layer_id], return_format="Array")
            if not rows:
                raise ServicesError("Reference layer not found")
            role = (self.get_secure_cookie("role") or b"").decode()
            if role != "Admin" and rows[0]["created_by"] != self.get_current_user():
                raise ServicesError("Only the uploader or an Admin can delete this layer")
            # shapes go with it (ON DELETE CASCADE)
            await self.pg.execute("DELETE FROM bioprotect.reference_layers WHERE id = %s", [layer_id])
            self.send_response({"info": "Reference layer deleted", "id": layer_id})
        except (ServicesError, KeyError, ValueError) as e:
            raise_error(self, e.args[0] if isinstance(e, ServicesError) else "Missing or invalid id")


class ImportReferenceLayerWSHandler(SocketHandler):
    """Unzips an uploaded shapefile, loads it into a scratch table and copies it
    into reference_layer_shapes in batches. All-or-nothing: a failure removes
    the layer row (and its shapes) and the scratch table. The zip and its
    unzipped files are always deleted."""

    def initialize(self, pg, get_unique_feature_name):
        super().initialize(pg=pg)
        self.proj_paths = get_folder_path_config()
        self.get_unique_feature_name = get_unique_feature_name

    async def run(self):
        try:
            await self.start({'info': "Importing reference layer.."})
        except ServicesError:  # authentication/authorisation error
            return

        folder = self.proj_paths.IMPORT_FOLDER
        zipfile = os.path.basename(self.get_argument('zipfile', ''))
        root = None
        scratch = None
        layer_id = None

        async def cleanup():
            """Idempotent: drops an unfinished layer, the scratch table and the files."""
            nonlocal layer_id, scratch, zipfile
            if layer_id is not None:  # shapes go with it (ON DELETE CASCADE)
                await self.pg.execute("DELETE FROM bioprotect.reference_layers WHERE id = %s",
                                      [layer_id])
                layer_id = None
            if scratch:
                await self.pg.execute(sql.SQL("DROP TABLE IF EXISTS bioprotect.{}")
                                      .format(sql.Identifier(scratch)))
                scratch = None
            if zipfile:
                delete_zipped_shapefile(folder, zipfile, root or os.path.splitext(zipfile)[0])
                zipfile = None

        try:
            name = (self.get_argument('name', '') or '').strip()
            description = (self.get_argument('description', '') or '').strip() or None
            if not zipfile:
                raise ServicesError("Missing required argument: zipfile")
            root = unzip_shapefile(folder, zipfile)
            if not name:
                name = root
            if await self.pg.execute(
                    "SELECT 1 FROM bioprotect.reference_layers WHERE lower(name) = lower(%s)",
                    [name], return_format="Array"):
                raise ServicesError(f"A reference layer called '{name}' already exists")
            # Every user sees the layer, so no guessing the CRS (unlike features)
            if not os.path.exists(os.path.join(folder, root + '.prj')):
                raise ServicesError(
                    f"'{root}.shp' has no .prj file, so its coordinate system is unknown. "
                    "Re-export the shapefile with its .prj file and upload it again.")

            scratch = self.get_unique_feature_name("scratch_")
            self.send_response({'status': 'Preprocessing', 'info': "Loading the shapefile.."})
            await self.pg.import_shapefile(folder, root + '.shp', scratch)

            types = await self.pg.execute(
                sql.SQL("SELECT DISTINCT ST_GeometryType(geometry) AS t FROM bioprotect.{} "
                        "WHERE geometry IS NOT NULL").format(sql.Identifier(scratch)),
                return_format="Array")
            kinds = {GEOM_TYPES.get(r["t"]) for r in types}
            if not kinds:
                raise ServicesError("The shapefile has no shapes")
            if None in kinds or len(kinds) > 1:
                raise ServicesError(
                    "The shapefile must hold only polygons, only lines or only points "
                    f"(found {', '.join(sorted(r['t'] for r in types))})")
            geom_type = kinds.pop()
            await self.pg.execute(sql.SQL("ALTER TABLE bioprotect.{} ADD COLUMN _ref_id serial")
                                  .format(sql.Identifier(scratch)))

            try:
                rows = await self.pg.execute(
                    """
                    INSERT INTO bioprotect.reference_layers
                        (name, description, geom_type, colour, created_by)
                    VALUES (%s, %s, %s, '', %s) RETURNING id
                    """, [name, description, geom_type, self.get_current_user()],
                    return_format="Array")
            except ServicesError as e:
                if e.args[0] == "That item already exists":  # lost a race on the name
                    raise ServicesError(f"A reference layer called '{name}' already exists") from e
                raise
            layer_id = rows[0]["id"]
            await self.pg.execute("UPDATE bioprotect.reference_layers SET colour = %s WHERE id = %s",
                                  [PALETTE[(layer_id - 1) % len(PALETTE)], layer_id])

            batches = await self.pg.execute(
                "SELECT from_id, to_id FROM bioprotect.reference_layer_batches(%s, %s)",
                [scratch, BATCH_POINTS], return_format="Array")
            for i, b in enumerate(batches, 1):
                self.send_response({'status': 'Preprocessing',
                                    'info': f"Preparing shapes ({i}/{len(batches)}).."})
                await self.pg.execute(
                    "SELECT bioprotect.reference_layer_add_shapes(%s, %s, %s, %s, %s)",
                    [layer_id, scratch, geom_type, b["from_id"], b["to_id"]])

            done_id, layer_id = layer_id, None  # finished: keep it
            await cleanup()
            self.close({'info': f"Reference layer '{name}' imported", 'id': done_id,
                        'layers': await _layer_rows(self.pg)})
        except ServicesError as e:
            await cleanup()  # before the client hears it failed
            self.send_response({'status': 'Finished', 'error': str(e),
                                'info': 'Failed to import the reference layer'})
            self.close(clean=False)
        finally:
            await cleanup()  # unexpected error: still all-or-nothing
