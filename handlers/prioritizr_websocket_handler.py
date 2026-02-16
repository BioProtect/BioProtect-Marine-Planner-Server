import asyncio
import os
import json
from services.service_error import ServicesError, raise_error
from handlers.websocket_handler import SocketHandler

# server/prioritizr?action=run&user=<id>&project_id=<pid>


class PrioritizrWSHandler(SocketHandler):
    """
    WebSocket handler to run Prioritizr (R) for a project and stream progress.
    """

    def initialize(self, pg, r_script_path):
        super().initialize(pg=pg)
        self.r_script_path = r_script_path
        self.proc = None
        self.run_id = None

    async def open(self):
        try:
            await super().open({"info": "Running Prioritizr..."})
        except ServicesError as e:
            self.send_response({
                "status": "Error",
                "info": "Error runing websocket...",
                "error": e
            })
            return

        # === validate ===
        self.validate_args(self.request.arguments, ["user", "project_id"])
        project_id = int(self.get_argument("project_id"))

        params_raw = self.get_argument("params", None)
        params = json.loads(params_raw) if params_raw else {}

        # === create run ===
        row = await self.pg.execute(
            """
            INSERT INTO bioprotect.prioritizr_runs (project_id, status, params)
            VALUES (%s, 'queued', %s::jsonb)
            RETURNING id
            """,
            data=[project_id, json.dumps(params or {})],
            return_format="Dict",
        )

        self.run_id = row[0]["id"]

        self.send_response({
            "status": "Queued",
            "run_id": self.run_id
        })

        # === prepare DB input ===
        await self.pg.execute(
            "UPDATE bioprotect.prioritizr_runs SET status='preparing' WHERE id=%s",
            data=[self.run_id],
        )

        self.send_response({
            "status": "Preparing",
            "info": "Preparing prioritizr input in PostGIS...",
            "run_id": self.run_id
        })

        await self.pg.execute(
            "SELECT bioprotect.prepare_prioritizr_input(%s)",
            data=[self.run_id],
        )

        # === start R ===
        await self.pg.execute(
            "UPDATE bioprotect.prioritizr_runs SET status='running' WHERE id=%s",
            data=[self.run_id],
        )

        self.send_response({
            "status": "Running",
            "info": "Launching Prioritizr...",
            "run_id": self.run_id
        })

        env = os.environ.copy()
        cmd = ["Rscript", self.r_script_path, str(self.run_id)]

        self.proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )

        await asyncio.gather(
            self._stream_output("stdout", self.proc.stdout),
            self._stream_output("stderr", self.proc.stderr),
        )

        rc = await self.proc.wait()

        if rc != 0:
            await self.pg.execute(
                """
                UPDATE bioprotect.prioritizr_runs
                SET status='failed', error=%s
                WHERE id=%s
                """,
                data=[f"R exited with code {rc}", self.run_id],
            )
            self.close({
                "status": "Failed",
                "run_id": self.run_id,
                "error": f"R exited with code {rc}"
            })
            return

        await self.pg.execute(
            "UPDATE bioprotect.prioritizr_runs SET status='done' WHERE id=%s",
            data=[self.run_id],
        )

        self.close({
            "status": "Finished",
            "run_id": self.run_id,
            "info": "Prioritizr run completed"
        })

    async def _stream_output(self, stream_name, stream):
        while True:
            line = await stream.readline()
            if not line:
                break

            msg = line.decode("utf-8", errors="replace").rstrip("\n")

            await self.pg.execute(
                """
                INSERT INTO bioprotect.prioritizr_run_logs (run_id, stream, message)
                VALUES (%s, %s, %s)
                """,
                data=[self.run_id, stream_name, msg],
            )

            self.send_response({
                "status": "Running",
                "stream": stream_name,
                "message": msg
            })

    def on_close(self):
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.kill()
            except Exception:
                pass
