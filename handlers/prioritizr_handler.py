# handlers/prioritizr_handler.py

from handlers.base_handler import BaseHandler
from services.service_error import ServicesError


class PrioritizrHandler(BaseHandler):

    def initialize(self, pg):
        super().initialize(pg=pg)

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

        self.write({"error": f"Unknown action '{action}'"})
        self.set_status(400)

    # --------------------------------------------------
    async def list_runs(self):
        self.validate_args(self.request.arguments, ["project-id"])
        project_id = self.get_argument("project-id")

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
                WHERE project_id = %s
                ORDER BY created_at DESC
                """)

        data = await self.pg.execute(query, data=[project_id], return_format="DataFrame")
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
