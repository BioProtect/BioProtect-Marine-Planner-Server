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
