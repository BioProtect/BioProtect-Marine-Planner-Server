"""
Upload a preprocessed raster as a cost profile for a project.

Three handlers live here:

  * GetRasterBandInfoHandler (REST)
      Inspect an uploaded raster sitting in data/tmp/ and return band
      count, dtypes, nodata, bounds, CRS. The frontend uses this to
      populate the band picker before submitting the WebSocket upload.

  * UploadRasterCostHandler (WebSocket)
      Clips + reprojects the raster to WGS84, runs exactextract zonal
      stats against EVERY hex the raster footprint covers (all
      resolutions, all project areas - not just this project's), caches
      those raw values in raster_hex_values, deletes the file, then
      builds a cost profile for the requesting project from the cache.

  * CostRasterLibraryHandler (REST)
      List / reuse / share / delete cached rasters. "Reuse" builds a
      cost profile for another project straight from the cache: no
      upload, no raster IO.

The raster file itself is never kept. What survives an upload is the
per-hex raw statistic, which is what every later project actually needs.
Reuse is gated on cost_rasters.owner_id + visibility (default private),
because a cache is consulted implicitly - shared-by-default would leak
one user's data into another user's project without either of them
choosing it.
"""

import hashlib
import json
import logging
import os

from classes.folder_path_config import get_folder_path_config
from handlers.base_handler import BaseHandler
from handlers.websocket_handler import SocketHandler
from services.raster_service import (
    SUPPORTED_STATS,
    bounds_of_hexes_in_bounds,
    bounds_wgs84,
    clip_and_reproject,
    extract_raster_to_hexes,
    fetch_hex_points_in_bounds,
    fetch_hexes_in_bounds,
    get_raster_band_info,
    normalise_costs,
    pick_sampling_method,
    pixel_area_km2,
    sample_raster_at_centroids,
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

    Extracts the raster against every hex its footprint covers - all
    resolutions, all project areas - caches those raw values, deletes the
    file, then builds a cost profile for the requesting project from the
    cache. Re-uploading the same file (same band + stat, same owner) skips
    extraction entirely and tops up nothing: the cache already has it.

    Required query args:
        project_id    int
        filename      raster file in data/tmp/
        profile_name  display name for the new cost profile

    Optional:
        description       text (default '')
        band              1-based band index (default 1)
        stat              one of SUPPORTED_STATS (default 'mean'). The
                          'mean' stat is already area-weighted by
                          fractional pixel coverage in exactextract;
                          there is no need for 'weighted_mean' here.
        normalise         'true'|'false' (default 'true')
        floor             float in (0, 1) (default 0.001)
        fill_strategy     'median'|'floor'|'max'|<float> (default 'median')
        set_active        'true'|'false' (default 'true')
        sampling          'auto'|'exact'|'centroid' (default 'auto').
                          'auto' uses a centroid lookup for resolutions
                          whose hexes sit well inside one pixel, where it
                          agrees with the area-weighted mean anyway, and
                          exactextract for the rest. 'exact' forces
                          exactextract everywhere - slower, but keeps the
                          blend for hexes straddling a pixel boundary.

    Negative pixel values are always clamped to floor in the output
    (not configurable — negatives are invalid for a cost layer).
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
            stat = self.get_argument("stat", "mean")
            if stat not in SUPPORTED_STATS:
                stat = "mean"

            normalise_flag = _truthy(self.get_argument("normalise", "true"))
            floor = float(self.get_argument("floor", "0.001"))
            fill_strategy = self.get_argument("fill_strategy", "median")
            set_active = _truthy(self.get_argument("set_active", "true"))
            sampling = self.get_argument("sampling", "auto")
            if sampling not in ("auto", "exact", "centroid"):
                sampling = "auto"

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

            # Step 2: has this exact file already been extracted?
            # Owner-scoped on purpose: a global checksum match would tell
            # one user that another holds their file.
            checksum = _sha256(raster_path)
            owner_id = await self._owner_id()
            existing = await self.pg.execute(
                """
                SELECT id, name FROM bioprotect.cost_rasters
                 WHERE checksum = %s AND band = %s AND stat = %s
                   AND owner_id IS NOT DISTINCT FROM %s
                 LIMIT 1;
                """,
                data=[checksum, band, stat, owner_id],
                return_format="Array",
            )

            if existing:
                raster_id = existing[0]["id"]
                self.send_response({
                    "status": "Preprocessing",
                    "info": (
                        f"Already extracted as '{existing[0]['name']}' - "
                        f"reusing cached values, skipping the raster."
                    ),
                })
                _remove_files(raster_path)
            else:
                raster_id = await self._extract_and_cache(
                    raster_path=raster_path,
                    reprojected_path=reprojected_path,
                    filename=filename,
                    profile_name=profile_name,
                    description=description,
                    band=band,
                    stat=stat,
                    checksum=checksum,
                    owner_id=owner_id,
                    sampling=sampling,
                )

            # Step 3: build the profile for THIS project from the cache
            self.send_response({
                "status": "Preprocessing",
                "info": "Building cost profile for this project...",
            })
            result = await build_profile_from_cache(
                pg=self.pg,
                project_id=project_id,
                raster_id=raster_id,
                profile_name=profile_name,
                description=description,
                floor=floor,
                normalise=normalise_flag,
                fill_strategy=fill_strategy,
                set_active=set_active,
                created_by=self.get_current_user(),
            )

            info = result["info"]
            self.close(close_message={
                "info": (
                    f"Cost profile created from raster "
                    f"({info['covered']} of {info['total']} hexes covered "
                    f"= {info['coverage_pct']:.1f}%)."
                ),
                "cost_profile_id": result["cost_profile_id"],
                "raster_id": raster_id,
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

    async def _owner_id(self):
        """Numeric user id, or None while auth enforcement is off."""
        try:
            return await self._get_authenticated_user_id()
        except AttributeError:
            # SocketHandler does not inherit BaseHandler's helper.
            uid = self.get_secure_cookie("user_id")
            if not uid:
                return None
            try:
                return int(uid.decode() if isinstance(uid, (bytes, bytearray)) else uid)
            except (ValueError, AttributeError):
                return None
        except ServicesError:
            return None

    async def _extract_and_cache(
        self,
        raster_path: str,
        reprojected_path: str,
        filename: str,
        profile_name: str,
        description: str,
        band: int,
        stat: str,
        checksum: str,
        owner_id,
        sampling: str = "auto",
    ) -> int:
        """Extract against every hex the raster covers, then bin the file.

        The extraction target is deliberately wider than the requesting
        project: every h3_cell intersecting the raster footprint, at every
        resolution. That is what makes the same upload reusable for a
        different grid or resolution later without keeping the raster.

        Resolutions are split by how the hex compares to a pixel. Where a
        hex is far smaller than a pixel, exactextract's area weighting is
        averaging a single value millions of times, so those resolutions
        take a vectorised centroid lookup instead. Resolutions with hexes
        big enough to span pixels still go through exactextract.

        Returns:
            The new bioprotect.cost_rasters.id.
        """
        self.send_response({
            "status": "Preprocessing",
            "info": "Finding planning hexes covered by this raster...",
        })
        raster_bounds = bounds_wgs84(raster_path)

        resolutions = await self.pg.execute(
            """
            SELECT hc.resolution AS resolution, COUNT(DISTINCT hc.h3_index) AS n
              FROM bioprotect.h3_cells hc
             WHERE hc.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
             GROUP BY hc.resolution
             ORDER BY 1;
            """,
            data=list(raster_bounds),
            return_format="Array",
        )
        if not resolutions:
            raise ServicesError(
                "The raster does not overlap any existing planning grid. "
                "Create the planning grid first, then upload the raster."
            )

        # Clip to the hexes before reprojecting - computed in PostGIS so we
        # never load geometries just to find a bounding box.
        hex_envelope = await bounds_of_hexes_in_bounds(self.pg, raster_bounds)
        self.send_response({
            "status": "Preprocessing",
            "info": "Clipping and reprojecting raster to WGS84...",
        })
        clip_and_reproject(raster_path, reprojected_path, hex_envelope)

        # Now the raster is in its final form, so a pixel has a real size.
        pix_area = pixel_area_km2(reprojected_path)
        centroid_res, exact_res = [], []
        for r in resolutions:
            target = (
                centroid_res
                if sampling == "centroid"
                else exact_res if sampling == "exact"
                else (
                    centroid_res
                    if pick_sampling_method(pix_area, r["resolution"]) == "centroid"
                    else exact_res
                )
            )
            target.append(r)

        total_hexes = sum(r["n"] for r in resolutions)
        self.send_response({
            "status": "Preprocessing",
            "info": (
                f"{total_hexes:,} hexes across resolution(s) "
                f"{', '.join(str(r['resolution']) for r in resolutions)} "
                f"- extracting once for all of them. "
                f"Pixel ~{pix_area:.1f} km²: "
                f"{_res_summary(centroid_res, 'centroid sample')}"
                f"{' + ' if centroid_res and exact_res else ''}"
                f"{_res_summary(exact_res, 'zonal stats')}."
            ),
        })

        extracted = []

        # Cheap path: one numpy lookup for every sub-pixel hex.
        if centroid_res:
            res_values = [r["resolution"] for r in centroid_res]
            self.send_response({
                "status": "Preprocessing",
                "info": (
                    f"Sampling {sum(r['n'] for r in centroid_res):,} hex "
                    f"centroids at resolution(s) "
                    f"{', '.join(map(str, res_values))}..."
                ),
            })
            points = await fetch_hex_points_in_bounds(
                self.pg, raster_bounds, res_values
            )
            extracted.extend(
                sample_raster_at_centroids(reprojected_path, points, band)
            )

        # Exact path: hexes big enough to span pixels.
        if exact_res:
            res_values = [r["resolution"] for r in exact_res]
            self.send_response({
                "status": "Preprocessing",
                "info": (
                    f"Running zonal stats ({stat}) for band {band} at "
                    f"resolution(s) {', '.join(map(str, res_values))}..."
                ),
            })
            hex_rows = await fetch_hexes_in_bounds(
                self.pg, raster_bounds, res_values
            )
            extracted.extend(
                await extract_raster_to_hexes(
                    raster_path=reprojected_path,
                    rows=hex_rows,
                    band=band,
                    stat=stat,
                )
            )

        covered = [e for e in extracted if not _is_nan(e["value"])]
        if not covered:
            raise ServicesError(
                "The raster produced no values for any planning hex."
            )

        # Record the raster, then the values, then delete the file.
        row = await self.pg.execute(
            """
            INSERT INTO bioprotect.cost_rasters
                (name, description, source_filename, band, stat, checksum,
                 bounds, owner_id, visibility, hex_count)
            VALUES (%s, %s, %s, %s, %s, %s,
                    ST_MakeEnvelope(%s, %s, %s, %s, 4326), %s, 'private', %s)
            RETURNING id;
            """,
            data=[
                profile_name, description, filename, band, stat, checksum,
                *raster_bounds, owner_id, len(covered),
            ],
            return_format="Array",
        )
        raster_id = row[0]["id"]

        self.send_response({
            "status": "Preprocessing",
            "info": f"Caching {len(covered):,} hex values...",
        })
        await _bulk_insert_hex_values(
            self.pg,
            [(raster_id, e["h3_index"], float(e["value"])) for e in covered],
        )

        _remove_files(raster_path, reprojected_path)
        return raster_id


def _res_summary(rows, label: str) -> str:
    if not rows:
        return ""
    res = ", ".join(str(r["resolution"]) for r in rows)
    return f"res {res} by {label}"


class CostRasterLibraryHandler(BaseHandler):
    """REST: /server/costRasters?action=...

    list            GET  - rasters this user may reuse (own + shared)
    create_profile  POST - build a cost profile for a project from a cached
                           raster. No upload, no raster IO.
    set_visibility  POST - flip a raster between 'private' and 'shared'
    delete          POST - drop a raster and its cached values
    """

    def initialize(self, pg=None):
        super().initialize(pg=pg)

    async def _owner_id(self):
        try:
            return await self._get_authenticated_user_id()
        except ServicesError:
            return None

    async def get(self):
        try:
            action = self.get_argument("action", "list")
            if action != "list":
                raise ServicesError(f"Unknown GET action: {action}")
            await self.list_rasters()
        except ServicesError as e:
            raise_error(self, e.args[0])
        except Exception as e:  # noqa: BLE001
            log.error("costRasters GET failed: %s", e, exc_info=True)
            raise_error(self, str(e))

    async def post(self):
        try:
            body = json.loads(self.request.body or "{}")
            action = body.get("action") or self.get_argument("action", "")
            if action == "create_profile":
                await self.create_profile(body)
            elif action == "set_visibility":
                await self.set_visibility(body)
            elif action == "delete":
                await self.delete_raster(body)
            else:
                raise ServicesError(f"Unknown POST action: {action}")
        except ServicesError as e:
            raise_error(self, e.args[0])
        except Exception as e:  # noqa: BLE001
            log.error("costRasters POST failed: %s", e, exc_info=True)
            raise_error(self, str(e))

    # ------------------------------------------------------------------
    async def list_rasters(self):
        """Rasters the user owns, plus any explicitly shared.

        Optional project_id reports how many of that project's hexes the
        cached values actually cover, so the user can tell before building
        a profile whether a raster reaches their area at their resolution.
        """
        owner_id = await self._owner_id()
        project_id = self.get_argument("project_id", None)

        rows = await self.pg.execute(
            """
            SELECT cr.id, cr.name, cr.description, cr.band, cr.stat,
                   cr.hex_count, cr.visibility, cr.owner_id,
                   cr.date_created, cr.source_filename,
                   (cr.owner_id IS NOT DISTINCT FROM %s) AS is_owner,
                   ST_XMin(cr.bounds) AS left, ST_YMin(cr.bounds) AS bottom,
                   ST_XMax(cr.bounds) AS right, ST_YMax(cr.bounds) AS top
              FROM bioprotect.cost_rasters cr
             WHERE cr.owner_id IS NOT DISTINCT FROM %s
                OR cr.visibility = 'shared'
             ORDER BY cr.date_created DESC;
            """,
            data=[owner_id, owner_id],
            return_format="Dict",
        )

        if project_id and rows:
            coverage = await self.pg.execute(
                """
                SELECT rhv.raster_id AS raster_id, COUNT(*) AS covered,
                       (SELECT COUNT(*) FROM bioprotect.project_pus
                         WHERE project_id = %s) AS total
                  FROM bioprotect.raster_hex_values rhv
                  JOIN bioprotect.project_pus pp
                    ON pp.h3_index = rhv.h3_index
                 WHERE pp.project_id = %s
                 GROUP BY rhv.raster_id;
                """,
                data=[int(project_id), int(project_id)],
                return_format="Array",
            )
            by_id = {c["raster_id"]: c for c in coverage}
            for r in rows:
                c = by_id.get(r["id"])
                total = (c or {}).get("total") or 0
                covered = (c or {}).get("covered") or 0
                r["project_covered"] = covered
                r["project_total"] = total
                r["project_coverage_pct"] = (
                    round(100.0 * covered / total, 1) if total else 0.0
                )

        self.send_response({"info": "Cost rasters returned", "data": rows})

    # ------------------------------------------------------------------
    async def create_profile(self, body):
        """Build a cost profile for a project from a cached raster."""
        raster_id = body.get("raster_id")
        project_id = body.get("project_id")
        if not raster_id or not project_id:
            raise ServicesError("raster_id and project_id are required.")

        await self._assert_readable(int(raster_id))

        floor = float(body.get("floor", 0.001))
        if floor <= 0 or floor >= 1:
            raise ServicesError("floor must be strictly between 0 and 1.")

        result = await build_profile_from_cache(
            pg=self.pg,
            project_id=int(project_id),
            raster_id=int(raster_id),
            profile_name=body.get("profile_name") or "Raster Cost Profile",
            description=body.get("description", ""),
            floor=floor,
            normalise=bool(body.get("normalise", True)),
            fill_strategy=body.get("fill_strategy", "median"),
            set_active=bool(body.get("set_active", True)),
            created_by=self.get_current_user(),
        )
        info = result["info"]
        self.send_response({
            "info": (
                f"Cost profile created from cached raster "
                f"({info['covered']} of {info['total']} hexes covered "
                f"= {info['coverage_pct']:.1f}%)."
            ),
            "cost_profile_id": result["cost_profile_id"],
            "coverage_pct": info["coverage_pct"],
            "covered": info["covered"],
            "total": info["total"],
            "fill_value": info["fill_value"],
        })

    async def set_visibility(self, body):
        """Only the owner may share or un-share their raster."""
        raster_id = int(body.get("raster_id") or 0)
        visibility = body.get("visibility")
        if visibility not in ("private", "shared"):
            raise ServicesError("visibility must be 'private' or 'shared'.")

        await self._assert_owner(raster_id)
        await self.pg.execute(
            "UPDATE bioprotect.cost_rasters SET visibility = %s WHERE id = %s;",
            data=[visibility, raster_id],
        )
        self.send_response({"info": f"Raster is now {visibility}."})

    async def delete_raster(self, body):
        """Delete a raster and its cached values (owner only).

        Profiles already built from it survive - source_raster_id is
        ON DELETE SET NULL - because their values are their own rows.
        """
        raster_id = int(body.get("raster_id") or 0)
        await self._assert_owner(raster_id)
        await self.pg.execute(
            "DELETE FROM bioprotect.cost_rasters WHERE id = %s;",
            data=[raster_id],
        )
        self.send_response({"info": "Cost raster deleted."})

    # ------------------------------------------------------------------
    async def _assert_readable(self, raster_id: int):
        owner_id = await self._owner_id()
        row = await self.pg.execute(
            """
            SELECT 1 FROM bioprotect.cost_rasters
             WHERE id = %s
               AND (owner_id IS NOT DISTINCT FROM %s OR visibility = 'shared');
            """,
            data=[raster_id, owner_id],
            return_format="Array",
        )
        if not row:
            raise ServicesError("Cost raster not found.")

    async def _assert_owner(self, raster_id: int):
        owner_id = await self._owner_id()
        row = await self.pg.execute(
            """
            SELECT 1 FROM bioprotect.cost_rasters
             WHERE id = %s AND owner_id IS NOT DISTINCT FROM %s;
            """,
            data=[raster_id, owner_id],
            return_format="Array",
        )
        if not row:
            raise ServicesError(
                "Cost raster not found, or you do not own it."
            )


# ----------------------------------------------------------------------------
# Shared: build a cost profile for a project out of cached raster values
# ----------------------------------------------------------------------------
async def build_profile_from_cache(
    pg,
    project_id: int,
    raster_id: int,
    profile_name: str,
    description: str,
    floor: float,
    normalise: bool,
    fill_strategy: str,
    set_active: bool,
    created_by: str,
) -> dict:
    """Join cached hex values onto a project's PUs, normalise, insert.

    Normalisation happens here rather than at extraction time on purpose:
    the log(X+1) rescale depends on the max over the hex set being
    normalised, so the same cached raster yields different (correct) costs
    for different projects.

    Hexes with no cached row are the raster's coverage gaps and arrive as
    NaN, which is exactly what fill_strategy handles.
    """
    rows = await pg.execute(
        """
        SELECT pp.id AS project_pu_id, rhv.value AS value
          FROM bioprotect.project_pus pp
          LEFT JOIN bioprotect.raster_hex_values rhv
                 ON rhv.h3_index = pp.h3_index
                AND rhv.raster_id = %s
         WHERE pp.project_id = %s
         ORDER BY pp.id;
        """,
        data=[raster_id, project_id],
        return_format="Array",
    )
    if not rows:
        raise ServicesError(f"Project {project_id} has no planning units.")

    values = [
        {
            "project_pu_id": r["project_pu_id"],
            "value": float("nan") if r["value"] is None else float(r["value"]),
        }
        for r in rows
    ]

    cost_map, info = normalise_costs(
        values=values,
        floor=floor,
        normalise=normalise,
        fill_strategy=fill_strategy,
    )
    if info["covered"] == 0:
        raise ServicesError(
            "That raster does not cover any of this project's planning "
            "units. It may not reach this area, or not at this resolution."
        )

    row = await pg.execute(
        """
        INSERT INTO bioprotect.cost_profiles
            (project_id, name, description, created_by, is_default,
             source_raster_id)
        VALUES (%s, %s, %s, %s, FALSE, %s)
        RETURNING id;
        """,
        data=[project_id, profile_name, description, created_by, raster_id],
        return_format="Array",
    )
    cost_profile_id = row[0]["id"]

    await _bulk_insert_profile_values(
        pg,
        [
            (cost_profile_id, pu_id, float(cost), 0)
            for pu_id, cost in cost_map.items()
        ],
    )

    if set_active:
        await pg.execute(
            "UPDATE bioprotect.projects "
            "SET active_cost_profile_id = %s WHERE id = %s;",
            data=[cost_profile_id, project_id],
        )

    return {"cost_profile_id": cost_profile_id, "info": info}


async def _bulk_insert_profile_values(pg, rows):
    """Insert (cost_profile_id, project_pu_id, cost, status) rows."""
    await _chunked_insert(
        pg,
        "INSERT INTO bioprotect.cost_profile_values "
        "(cost_profile_id, project_pu_id, cost, status) VALUES ",
        "(%s,%s,%s,%s)",
        rows,
    )


async def _bulk_insert_hex_values(pg, rows):
    """Insert (raster_id, h3_index, value) cache rows."""
    await _chunked_insert(
        pg,
        "INSERT INTO bioprotect.raster_hex_values "
        "(raster_id, h3_index, value) VALUES ",
        "(%s,%s,%s)",
        rows,
        suffix=" ON CONFLICT (raster_id, h3_index) DO NOTHING",
    )


async def _chunked_insert(pg, insert_sql, placeholder, rows, suffix=""):
    """Chunked multi-row INSERT, to stay under statement size limits."""
    if not rows:
        return
    chunk = 5000
    for i in range(0, len(rows), chunk):
        batch = rows[i: i + chunk]
        placeholders = ",".join([placeholder] * len(batch))
        flat = [v for r in batch for v in r]
        await pg.execute(insert_sql + placeholders + suffix, data=flat)


def _sha256(path: str) -> str:
    """Stream the file so a multi-GB raster does not land in memory."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _remove_files(*paths):
    for f in paths:
        try:
            if f and os.path.exists(f):
                os.remove(f)
        except OSError as exc:
            log.warning("Failed to remove %s: %s", f, exc)


def _is_nan(x) -> bool:
    try:
        return float(x) != float(x)
    except (TypeError, ValueError):
        return False


def _truthy(v) -> bool:
    if isinstance(v, bool):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode("utf-8")
    return str(v).strip().lower() in ("1", "true", "yes", "on")
