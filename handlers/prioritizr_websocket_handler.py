import asyncio
import json
import os
import pty

from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError, raise_error

# server/prioritizr?action=run&user=<id>&project_id=<pid>


class PrioritizrWSHandler(SocketHandler):
    """
    WebSocket handler to run Prioritizr (R) for a project and stream progress.
    """

    def initialize(self, pg, r_script_path):
        super().initialize(pg=pg)
        self.r_script_path = r_script_path
        self.proc = None
        self.master_fd = None
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
            "run_id": self.run_id,
            "info": f"Prioritizr run {self.run_id} queued. Preparing to start..."
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

        try:
            env = os.environ.copy()
            # cmd = ["Rscript", self.r_script_path, str(self.run_id)]
            cmd = [
                "Rscript", "--vanilla", "--slave",
                self.r_script_path,
                str(self.run_id)
            ]

            # --------------------------
            #  START R WITH A PSEUDO-TTY
            # --------------------------
            master_fd, slave_fd = pty.openpty()
            self.master_fd = master_fd

            self.proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=slave_fd,
                stdout=slave_fd,
                stderr=slave_fd,
                env=env,
                start_new_session=True,  # important
            )

            # Close slave FD in parent
            os.close(slave_fd)

            # Start streaming PTY output
            await self._read_pty_until_exit()
            rc = await self.proc.wait()
            await self._drain_pty()

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
                    "error": f"R exited with code {rc}",
                    "info": f"R exited with code {rc}",
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
        except Exception as e:
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
                "error": f"R exited with code {rc}",
                "info": f"R exited with code {rc}",
            })
            return

    async def _read_pty_until_exit(self):
        """Non-blocking read loop: stream PTY output while process is alive."""
        loop = asyncio.get_event_loop()
        while True:
            if self.proc.returncode is not None:
                break
            try:
                data = await loop.run_in_executor(None, os.read, self.master_fd, 1024)
            except OSError:
                break
            if data:
                await self._ingest_text(data.decode("utf-8", errors="replace"))
            else:
                await asyncio.sleep(0.02)

    async def _drain_pty(self):
        """After process exit, drain any remaining bytes in the PTY buffer."""
        loop = asyncio.get_event_loop()
        while True:
            try:
                data = await loop.run_in_executor(None, os.read, self.master_fd, 1024)
            except OSError:
                break
            if not data:
                break
            await self._ingest_text(data.decode("utf-8", errors="replace"))

    async def _ingest_text(self, text: str):
        # PTYs may split arbitrarily; splitlines keeps behaviour sane
        for line in text.splitlines():
            line = line.rstrip("\r\n")
            # Persist to DB
            await self.pg.execute(
                """
                INSERT INTO bioprotect.prioritizr_run_logs (run_id, stream, message)
                VALUES (%s, %s, %s)
                """,
                data=[self.run_id, "pty", line],
            )
            # Push over websocket
            self.send_response({
                "status": "Running",
                "stream": "pty",
                "message": line
            })

    def on_close(self):
        # Kill child process group; systemd KillMode=control-group helps too
        if self.proc and self.proc.returncode is None:
            try:
                self.proc.kill()
            except Exception:
                pass
        if self.master_fd:
            try:
                os.close(self.master_fd)
            except Exception:
                pass
