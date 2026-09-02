# handlers/prioritizr_handler.py

import logging
import os
import shutil

from handlers.base_handler import BaseHandler
from services.run_export_service import ALLOWED_FORMATS, build_export_zip
from services.service_error import ServicesError


# Who may delete a run: the user who started it, an owner of its project, or
# a global Admin. Defined once — list_runs reports it, delete_run enforces it.
# Takes the current user id three times, positionally.
_MAY_DELETE_SQL = """
        COALESCE(r.created_by = %s, FALSE)
        OR EXISTS (SELECT 1 FROM bioprotect.user_projects up
                   WHERE up.project_id = r.project_id
                     AND up.user_id = %s AND up.role = 'owner')
        OR EXISTS (SELECT 1 FROM bioprotect.users au
                   WHERE au.id = %s AND au.role = 'Admin')
"""


class PrioritizrHandler(BaseHandler):

    def initialize(self, pg):
        super().initialize(pg=pg)

    async def _session_user_id(self):
        """The signed-in user id, or None when the request is anonymous."""
        try:
            return await self._get_authenticated_user_id()
        except ServicesError:
            return None

    def validate_args(self, args, required_keys):
        """Checks that all of the arguments in argumentList are in the arguments dictionary."""
        missing = [key for key in required_keys if key not in args]
        if missing:
            raise ServicesError(
                f"Missing required arguments: {', '.join(missing)}")

    async def get(self):
        action = self.get_argument("action", None)

        if action == "list-runs":
            return await self.list_runs()

        if action == "get-run":
            return await self.get_run()

        if action == "get-results":
            return await self.get_results()

        if action == "get-feature-representation":
            return await self.get_feature_representation()

        if action == "export-runs":
            return await self.export_runs()

        self.write({"error": f"Unknown action '{action}'"})
        self.set_status(400)

    async def post(self):
        action = self.get_argument("action", None)

        if action == "delete-run":
            return await self.delete_run()

        self.set_status(400)
        self.write({"error": f"Unknown action '{action}'"})

    # --------------------------------------------------
    # POST /prioritizr?action=delete-run&run-id=<id>
    async def delete_run(self):
        """Deletes a run. Only the user who started it may do so."""
        try:
            run_id = int(self.get_argument("run-id", ""))
        except ValueError:
            self.set_status(400)
            self.write({"error": "run-id must be an integer"})
            return

        try:
            user_id = await self._get_authenticated_user_id()
        except ServicesError as e:
            self.set_status(401)
            self.write({"error": e.args[0]})
            return

        # Permission is enforced in the WHERE clause, so there is no gap
        # between the check and the delete. Logs and results rows go with it
        # via ON DELETE CASCADE, and the run's input table via the
        # trg_drop_prioritizr_input_table trigger.
        rows = await self.pg.execute(
            f"""
            DELETE FROM bioprotect.prioritizr_runs AS r
            WHERE r.id = %s AND ({_MAY_DELETE_SQL})
            RETURNING r.id
            """,
            data=[run_id, user_id, user_id, user_id],
            return_format="Dict",
        )

        if not rows:
            self.set_status(403)
            self.write({
                "error": "Only the user who started this run, a project "
                         "owner, or an admin can delete it"
            })
            return

        self.send_response({"info": f"Run {run_id} deleted", "id": run_id})

    # --------------------------------------------------
    async def list_runs(self):
        self.validate_args(self.request.arguments, ["project-id"])
        project_id = self.get_argument("project-id")

        user_id = await self._session_user_id()

        query = (f"""
                SELECT
                    ({_MAY_DELETE_SQL}) AS can_delete,
                    r.id,
                    r.project_id,
                    r.created_by,
                    u.username AS created_by_name,
                    r.created_at,
                    r.status,
                    r.params,
                    r.input_table,
                    r.feature_cols,
                    r.feature_map,
                    r.label,
                    r.description
                FROM bioprotect.prioritizr_runs r
                LEFT JOIN bioprotect.users u ON u.id = r.created_by
                WHERE r.project_id = %s
                ORDER BY r.created_at DESC
                """)

        data = await self.pg.execute(
            query, data=[user_id, user_id, user_id, project_id],
            return_format="DataFrame")
        self.send_response({"data": data.to_dict(orient="records")})

    # --------------------------------------------------
    async def get_run(self):
        self.validate_args(self.request.arguments, ["run-id"])
        run_id = self.get_argument("run-id")

        query = ("""
                SELECT
                    id,
                    project_id,
                    created_by,
                    created_at,
                    status,
                    params,
                    input_table,
                    feature_cols,
                    feature_map,
                    label,
                    description
                FROM bioprotect.prioritizr_runs
                WHERE id = %s
                """)
        data = await self.pg.execute(query, data=[run_id], return_format="DataFrame")
        self.send_response({"info": "Run returned",
                            "data": data.to_dict(orient="records")})

    # --------------------------------------------------
    async def get_feature_representation(self):
        """
        For one or more run IDs, compute how much of each project feature is
        represented in the solution (selected hexes).  When multiple runs are
        supplied the representation is averaged across them so the gauge shows
        the mean achievement across the currently visualised runs.

        Query params:
            run-ids  — comma-separated list of integer run IDs (e.g. "12,14,15")
        """
        raw = self.get_argument("run-ids", "")
        try:
            run_ids = [int(r.strip()) for r in raw.split(",") if r.strip()]
        except ValueError:
            self.set_status(400)
            self.write({"error": "run-ids must be comma-separated integers"})
            return

        if not run_ids:
            self.set_status(400)
            self.write({"error": "run-ids is required"})
            return

        query = """
            WITH run_info AS (
                -- Use the project from the first supplied run; all runs must
                -- belong to the same project.
                SELECT project_id
                FROM   bioprotect.prioritizr_runs
                WHERE  id = ANY(%s)
                LIMIT  1
            ),
            totals AS (
                -- Total amount of each feature across ALL project hexes
                SELECT pfa.feature_unique_id,
                       SUM(pfa.amount) AS total_amount
                FROM   bioprotect.pu_feature_amounts pfa
                JOIN   run_info ON pfa.project_id = run_info.project_id
                GROUP  BY pfa.feature_unique_id
            ),
            per_run AS (
                -- Amount of each feature captured in the solution per run
                SELECT rr.run_id,
                       pfa.feature_unique_id,
                       SUM(pfa.amount) AS represented_amount
                FROM   bioprotect.pu_feature_amounts pfa
                JOIN   run_info ON pfa.project_id = run_info.project_id
                JOIN   bioprotect.prioritizr_run_results rr
                         ON  rr.h3_index = pfa.h3_index
                         AND rr.run_id   = ANY(%s)
                         AND rr.solution = 1
                GROUP  BY rr.run_id, pfa.feature_unique_id
            ),
            averaged AS (
                -- Average across all selected runs
                SELECT feature_unique_id,
                       AVG(represented_amount) AS avg_represented
                FROM   per_run
                GROUP  BY feature_unique_id
            ),
            per_run_pct AS (
                -- Per-run represented percent (for tri-state mixed detection)
                SELECT pr.feature_unique_id,
                       pr.run_id,
                       CASE
                           WHEN COALESCE(t.total_amount, 0) > 0
                           THEN ROUND(
                                    (pr.represented_amount / t.total_amount * 100)::numeric,
                                    2)
                           ELSE 0
                       END AS represented_percent
                FROM   per_run pr
                LEFT JOIN totals t ON t.feature_unique_id = pr.feature_unique_id
            ),
            per_run_json AS (
                SELECT feature_unique_id,
                       json_agg(
                           json_build_object(
                               'run_id',              run_id,
                               'represented_percent', represented_percent
                           )
                           ORDER BY run_id
                       ) AS per_run
                FROM   per_run_pct
                GROUP  BY feature_unique_id
            )
            SELECT
                pf.feature_unique_id,
                mif.alias                                          AS feature_name,
                pf.target_value,
                COALESCE(t.total_amount,  0)                       AS total_amount,
                COALESCE(a.avg_represented, 0)                     AS represented_amount,
                CASE
                    WHEN COALESCE(t.total_amount, 0) > 0
                    THEN ROUND(
                             (COALESCE(a.avg_represented, 0)
                              / t.total_amount * 100)::numeric, 2)
                    ELSE 0
                END                                                AS represented_percent,
                COALESCE(prj.per_run, '[]'::json)                  AS per_run
            FROM   bioprotect.project_features pf
            JOIN   run_info ON pf.project_id = run_info.project_id
            JOIN   bioprotect.metadata_interest_features mif
                     ON mif.unique_id = pf.feature_unique_id
            LEFT JOIN totals t ON t.feature_unique_id = pf.feature_unique_id
            LEFT JOIN averaged a ON a.feature_unique_id = pf.feature_unique_id
            LEFT JOIN per_run_json prj ON prj.feature_unique_id = pf.feature_unique_id
            ORDER  BY mif.alias
        """

        data = await self.pg.execute(
            query,
            data=[run_ids, run_ids],
            return_format="DataFrame"
        )

        self.send_response({
            "info": "Feature representation returned",
            "run_ids": run_ids,
            "data": data.to_dict(orient="records"),
        })

    # --------------------------------------------------
    async def get_results(self):
        self.validate_args(self.request.arguments, ["run-id"])
        run_id = self.get_argument("run-id", None)
        query = (
            """
                SELECT
                    h3_index,
                    solution
                FROM bioprotect.prioritizr_run_results
                WHERE run_id = %s
                """)

        data = await self.pg.execute(query, data=[run_id], return_format="DataFrame")
        print('Results data: ', data)
        self.send_response({"info": "Results returned",
                            "run_id": int(run_id),
                            "data": data.to_dict(orient="records")})

    # --------------------------------------------------
    async def export_runs(self):
        """Stream a zipped GIS export (shapefile or geopackage) of the
        project's planning-unit grid with selection results from the
        supplied Prioritizr runs joined on per-PU."""
        self.validate_args(self.request.arguments, ["project-id", "run-ids"])

        try:
            project_id = int(self.get_argument("project-id"))
        except ValueError:
            self.set_status(400)
            self.write({"error": "project-id must be an integer"})
            return

        raw = self.get_argument("run-ids", "")
        try:
            run_ids = [int(r.strip()) for r in raw.split(",") if r.strip()]
        except ValueError:
            self.set_status(400)
            self.write({"error": "run-ids must be comma-separated integers"})
            return
        if not run_ids:
            self.set_status(400)
            self.write({"error": "run-ids is required"})
            return

        fmt = self.get_argument("format", "shp").lower()
        if fmt not in ALLOWED_FORMATS:
            self.set_status(400)
            self.write({"error": f"format must be one of {ALLOWED_FORMATS}"})
            return

        zip_path, work_folder = await build_export_zip(
            self.pg, self.proj_paths.EXPORT_FOLDER, project_id, run_ids, fmt,
        )
        try:
            download_name = (
                f"project_{project_id}_runs_{fmt}.zip"
            )
            self.set_header("Content-Type", "application/zip")
            self.set_header(
                "Content-Disposition",
                f'attachment; filename="{download_name}"',
            )

            # Stream in 1 MB chunks so large grids don't blow up memory.
            CHUNK = 1024 * 1024
            with open(zip_path, "rb") as f:
                while True:
                    block = f.read(CHUNK)
                    if not block:
                        break
                    self.write(block)
                    await self.flush()
        finally:
            shutil.rmtree(work_folder, ignore_errors=True)
            try:
                os.remove(zip_path)
            except OSError as e:
                logging.warning("Failed to remove export zip %s: %s", zip_path, e)
