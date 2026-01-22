import asyncio
import os
import json
from services.service_error import ServicesError, raise_error
from handlers.websocket_handler import SocketHandler

# server/prioritizr?action=run&user=<id>&project_id=<pid>


class PrioritizrHandler(SocketHandler):
    """
    WebSocket handler to run Prioritizr (R) for a project and stream progress logs.
    """

    def initialize(self, pg, r_script_path):
        super().initialize()
        self.pg = pg
        self.r_script_path = r_script_path
        self.proc = None
        self.run_id = None

    async def open(self):
        try:
            await super().open({"info": "Starting prioritizr run..."})
        except ServicesError:
            pass

    async def on_message(self, message):
        action = self.get_argument("action", None)
        try:
            if action == "run":
                await self.run_prioritizr()
            else:
                raise ServicesError("Invalid action specified.")
        except ServicesError as e:
            raise_error(self, e.args[0])

    async def _log(self, stream, msg):
        msg = (msg or "").rstrip("\n")
        if not msg:
            return

        # persist logs (optional but recommended)
        if self.run_id:
            await self.pg.execute(
                """
                INSERT INTO bioprotect.prioritizr_run_logs(run_id, stream, message)
                VALUES (%s, %s, %s)
                """,
                [self.run_id, stream, msg],
            )

        # push to UI
        self.send_response(
            {"status": "Running", "stream": stream, "message": msg})

    async def _pump_stream(self, stream_name, stream):
        while True:
            line = await stream.readline()
            if not line:
                break
            await self._log(stream_name, line.decode("utf-8", errors="replace"))

    async def _create_run_row(self, project_id, params):
        # each click => new run_id so you can compare scenarios later
        row = await self.pg.execute(
            """
            INSERT INTO bioprotect.prioritizr_runs(project_id, status, params)
            VALUES (%s, 'queued', COALESCE(%s::jsonb, '{}'::jsonb))
            RETURNING id
            """,
            [project_id, json.dumps(params or {})],
            return_format="Dict",
        )
        if not row:
            raise ServicesError("Failed to create prioritizr run row")
        return int(row[0]["id"])

    async def run_prioritizr(self):
        self.validate_args(self.request.arguments, ["user", "project_id"])
        project_id = int(self.get_argument("project_id"))

        # Optional: allow the client to pass scenario params (targets, penalties, mode, solver)
        # If you don't want this yet, just keep params = {}
        params_raw = self.get_argument("params", None)
        params = json.loads(params_raw) if params_raw else {}

        # 1) Create run_id
        self.run_id = await self._create_run_row(project_id, params)
        self.send_response({"status": "Queued", "run_id": self.run_id})

        # 2) Prepare input in DB
        await self.pg.execute(
            "UPDATE bioprotect.prioritizr_runs SET status='preparing' WHERE id=%s",
            [self.run_id],
        )
        self.send_response(
            {"status": "Preparing", "info": "Preparing input table in PostGIS...", "run_id": self.run_id})

        await self.pg.execute(
            "SELECT bioprotect.prepare_prioritizr_input(%s)",
            [self.run_id],
        )

        # 3) Spawn R
        await self.pg.execute(
            "UPDATE bioprotect.prioritizr_runs SET status='running' WHERE id=%s",
            [self.run_id],
        )
        self.send_response(
            {"status": "Running", "info": "Launching prioritizr...", "run_id": self.run_id})

        # Ensure R can talk to the DB via env vars (recommended; don't pass secrets on CLI)
        env = os.environ.copy()
        # If you already set these in the service environment, you can omit these lines:
        # env["PGHOST"] = ...
        # env["PGPORT"] = ...
        # env["PGDATABASE"] = ...
        # env["PGUSER"] = ...
        # env["PGPASSWORD"] = ...

        cmd = ["Rscript", self.r_script_path, str(self.run_id)]

        self.proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        # 4) Stream stdout/stderr
        await asyncio.gather(
            self._pump_stream("stdout", self.proc.stdout),
            self._pump_stream("stderr", self.proc.stderr),
        )

        rc = await self.proc.wait()

        # 5) Finalize status
        if rc != 0:
            await self.pg.execute(
                "UPDATE bioprotect.prioritizr_runs SET status='failed', error=%s WHERE id=%s",
                [f"R exited with code {rc}", self.run_id],
            )
            self.close({"status": "Failed", "run_id": self.run_id,
                       "error": f"R exited with code {rc}"})
            return

        await self.pg.execute(
            "UPDATE bioprotect.prioritizr_runs SET status='done' WHERE id=%s",
            [self.run_id],
        )
        self.close({"status": "Finished", "run_id": self.run_id,
                   "info": "Prioritizr run completed"})

    def on_close(self):
        # If client disconnects, optionally terminate the process
        try:
            if self.proc and self.proc.returncode is None:
                self.proc.kill()
        except Exception:
            pass
