"""
Upload a preprocessed raster as a cost profile for a project.

Two handlers live here:

  * GetRasterBandInfoHandler (REST)
      Inspect an uploaded raster sitting in data/tmp/ and return band
      count, dtypes, nodata, bounds, CRS. The frontend uses this to
      populate the band picker before submitting the WebSocket upload.

  * UploadRasterCostHandler (WebSocket)
      Reprojects the raster to WGS84, runs exactextract zonal stats
      against the project's hexes, applies Halpern-style log(X+1)
      normalisation with a non-zero floor (so no cell can ever have
      cost = 0), and writes the result to cost_profiles +
      cost_profile_values. Optionally sets the new profile active.
"""

import logging
import os

from classes.folder_path_config import get_folder_path_config
from handlers.base_handler import BaseHandler
from handlers.websocket_handler import SocketHandler
from services.raster_service import (
    SUPPORTED_STATS,
    extract_raster_to_hexes,
    get_raster_band_info,
    normalise_costs,
    reproject_to_wgs84,
)
from services.service_error import ServicesError, raise_error


log = logging.getLogger(__name__)

project_paths = get_folder_path_config()

# The frontend FileUpload component sends destFolder="imports", which the
# uploadFileToFolder handler writes to <PROJECT_FOLDER>/imports/. The
# legacy activity raster path also hardcodes "data/tmp" relative to the
# server's cwd. We try the canonical IMPORT_FOLDER first and fall back to
# "data/tmp" for compatibility.
_TMP_FOLDER = "data/tmp"


def _resolve_uploaded_raster(filename: str) -> str:
    """Return the path of an uploaded raster, checking known locations."""
    candidates = [
        os.path.join(project_paths.IMPORT_FOLDER, filename),
        os.path.join(_TMP_FOLDER, filename),
    ]
    for p in candidates:
        if os.path.isfile(p):
            return p
    raise ServicesError(
        f"Raster file not found in any upload folder: {filename}. "
        f"Checked: {', '.join(candidates)}"
    )


class GetRasterBandInfoHandler(BaseHandler):
    """REST: GET /server/getRasterBandInfo?filename=<name>

    Returns metadata for a raster sitting in data/tmp/ so the frontend
    can decide whether to show a band picker.
    """

    def initialize(self, pg=None):
        super().initialize(pg=pg)

    async def get(self):
        try:
            if "filename" not in self.request.arguments:
                raise ServicesError("Missing required argument: filename")
            filename = self.get_argument("filename")
            raster_path = _resolve_uploaded_raster(filename)
            info = get_raster_band_info(raster_path)
            self.send_response({"info": "ok", "data": info})
        except ServicesError as e:
            raise_error(self, e.args[0])
        except Exception as e:  # noqa: BLE001
            log.error("getRasterBandInfo failed: %s", e, exc_info=True)
            raise_error(self, str(e))


class UploadRasterCostHandler(SocketHandler):
    """WebSocket: /server/uploadRasterCost

    Required query args:
        project_id    int
        filename      raster file in data/tmp/
        profile_name  display name for the new cost profile

    Optional:
        description       text (default '')
        band              1-based band index (default 1)
        stat              one of SUPPORTED_STATS (default 'weighted_mean')
        normalise         'true'|'false' (default 'true')
        clamp_negative    'true'|'false' (default 'true')
        floor             float in (0, 1) (default 0.001)
        fill_strategy     'median'|'floor'|'max'|<float> (default 'median')
        set_active        'true'|'false' (default 'true')
    """

    def initialize(self, pg):
        super().initialize(pg=pg)

    async def open(self):
        try:
            await super().open({"info": "Uploading raster cost profile..."})
        except ServicesError as e:
            log.error("UploadRasterCostHandler open failed: %s", e)
            return

        try:
            self.validate_args(
                self.request.arguments,
                ["project_id", "filename", "profile_name"],
            )

            project_id = int(self.get_argument("project_id"))
            filename = self.get_argument("filename")
            profile_name = self.get_argument("profile_name")
            description = self.get_argument("description", "")
            band = int(self.get_argument("band", "1"))
            stat = self.get_argument("stat", "weighted_mean")
            if stat not in SUPPORTED_STATS:
                stat = "weighted_mean"

            normalise_flag = _truthy(self.get_argument("normalise", "true"))
            clamp_negative = _truthy(self.get_argument("clamp_negative", "true"))
            floor = float(self.get_argument("floor", "0.001"))
            fill_strategy = self.get_argument("fill_strategy", "median")
            set_active = _truthy(self.get_argument("set_active", "true"))

            if floor <= 0 or floor >= 1:
                raise ServicesError(
                    "floor must be strictly between 0 and 1."
                )

            raster_path = _resolve_uploaded_raster(filename)
            # Reprojected copy lives next to the source.
            reprojected_path = os.path.join(
                os.path.dirname(raster_path),
                f"repro_{os.path.basename(raster_path)}",
            )

            # Step 1: validate project has planning units
            self.send_response({
                "status": "Preprocessing",
                "info": "Validating project planning units...",
            })
            check = await self.pg.execute(
                "SELECT COUNT(*) AS n FROM bioprotect.project_pus "
                "WHERE project_id = %s;",
                data=[project_id],
                return_format="Array",
            )
            if not check or check[0]["n"] == 0:
                raise ServicesError(
                    f"Project {project_id} has no planning units."
                )

            # Step 2: reproject to WGS84 if needed
            self.send_response({
                "status": "Preprocessing",
                "info": "Reprojecting raster to WGS84 if required...",
            })
            reproject_to_wgs84(raster_path, reprojected_path)

            # Step 3: exactextract zonal stats per project hex
            self.send_response({
                "status": "Preprocessing",
                "info": (
                    f"Running zonal stats ({stat}) for band {band} "
                    f"against project hexes..."
                ),
            })
            extracted = await extract_raster_to_hexes(
                raster_path=reprojected_path,
                project_id=project_id,
                band=band,
                stat=stat,
                pg=self.pg,
            )
            if not extracted:
                raise ServicesError(
                    "No project hexes were found for extraction."
                )

            # Step 4: Halpern normalisation with non-zero floor
            self.send_response({
                "status": "Preprocessing",
                "info": "Normalising values to [floor, 1]...",
            })
            cost_map, info = normalise_costs(
                values=extracted,
                floor=floor,
                normalise=normalise_flag,
                clamp_negative=clamp_negative,
                fill_strategy=fill_strategy,
            )

            # Step 5: insert cost profile + values
            self.send_response({
                "status": "Preprocessing",
                "info": "Writing cost profile to database...",
            })
            cost_profile_id = await self._insert_profile(
                project_id=project_id,
                profile_name=profile_name,
                description=description,
                cost_map=cost_map,
            )

            # Step 6: optionally set as active profile
            if set_active:
                await self.pg.execute(
                    "UPDATE bioprotect.projects "
                    "SET active_cost_profile_id = %s WHERE id = %s;",
                    data=[cost_profile_id, project_id],
                )

            # Step 7: cleanup temp files
            for f in (raster_path, reprojected_path):
                try:
                    if os.path.exists(f):
                        os.remove(f)
                except OSError as exc:
                    log.warning("Failed to remove %s: %s", f, exc)

            self.close(close_message={
                "info": (
                    f"Cost profile created from raster "
                    f"({info['covered']} of {info['total']} hexes covered "
                    f"= {info['coverage_pct']:.1f}%)."
                ),
                "cost_profile_id": cost_profile_id,
                "coverage_pct": info["coverage_pct"],
                "covered": info["covered"],
                "total": info["total"],
                "fill_value": info["fill_value"],
            })

        except ServicesError as e:
            self.close(close_message={
                "error": e.args[0],
                "info": "Failed to upload raster cost profile",
            })
        except Exception as e:  # noqa: BLE001
            log.error(
                "Unexpected error in UploadRasterCostHandler: %s",
                e,
                exc_info=True,
            )
            self.close(close_message={
                "error": str(e),
                "info": "Failed to upload raster cost profile",
            })

    async def _insert_profile(
        self,
        project_id: int,
        profile_name: str,
        description: str,
        cost_map: dict[int, float],
    ) -> int:
        """Insert cost_profiles row + bulk insert cost_profile_values.

        Returns the new cost_profile_id.
        """
        # Insert profile metadata
        row = await self.pg.execute(
            """
            INSERT INTO bioprotect.cost_profiles
                (project_id, name, description, created_by, is_default)
            VALUES (%s, %s, %s, %s, FALSE)
            RETURNING id;
            """,
            data=[
                project_id,
                profile_name,
                description,
                self.get_current_user(),
            ],
            return_format="Array",
        )
        cost_profile_id = row[0]["id"]

        # Bulk insert cost values via execute_values. The pg wrapper
        # ultimately exposes a raw psycopg2 connection; we fall back to
        # row-by-row inserts if execute_values is not directly callable.
        rows = [
            (cost_profile_id, pu_id, float(cost), 0)
            for pu_id, cost in cost_map.items()
        ]
        await self._bulk_insert_values(rows)
        return cost_profile_id

    async def _bulk_insert_values(self, rows):
        """Insert (cost_profile_id, project_pu_id, cost, status) rows.

        Uses pg.execute with a VALUES list. Chunks to avoid statement
        size limits for very large projects.
        """
        if not rows:
            return
        chunk = 5000
        insert_sql = (
            "INSERT INTO bioprotect.cost_profile_values "
            "(cost_profile_id, project_pu_id, cost, status) VALUES "
        )
        for i in range(0, len(rows), chunk):
            batch = rows[i: i + chunk]
            placeholders = ",".join(["(%s,%s,%s,%s)"] * len(batch))
            flat = [v for r in batch for v in r]
            await self.pg.execute(insert_sql + placeholders, data=flat)


def _truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8")
    return str(v).strip().lower() in ("1", "true", "yes", "on")
