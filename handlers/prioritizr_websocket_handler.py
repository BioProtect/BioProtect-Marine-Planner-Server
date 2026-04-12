import asyncio
import json
import os
import pty
import re

from handlers.websocket_handler import SocketHandler
from services.service_error import ServicesError, raise_error

# --------------------------------------------------------------------
# PTY output sanitisation
# --------------------------------------------------------------------
# R running under a PTY emits ANSI cursor-visibility sequences
# (e.g. "\x1b[?25h") and the CBC solver sprays further escape codes.
# Strip them all so the stored log is readable plain text.
#
# Also: the PTY read buffer may cut in the middle of a line, producing
# garbled fragments like ["Knapsack was ", "tried 0 times..."]. We
# buffer partial lines and only flush on newline to avoid that.

# Matches the vast majority of ANSI / VT100 escape sequences:
#  - CSI     : ESC [ ... <final byte>
#  - OSC     : ESC ] ... BEL / ESC \
#  - SS2/3   : ESC N / ESC O
#  - simple  : ESC <char>
_ANSI_RE = re.compile(
    r"""
    \x1B                           # ESC
    (?:
        \[[0-?]*[ -/]*[@-~]        # CSI sequence
      | \][^\x07\x1B]*(?:\x07|\x1B\\)  # OSC sequence
      | [@-Z\\-_]                  # single-char escape
    )
    """,
    re.VERBOSE,
)

# Stray control chars that aren't ANSI (backspace, bell, form feed, etc.)
_CTRL_RE = re.compile(r"[\x00-\x08\x0B-\x1F\x7F]")

# Lines that are pure solver bookkeeping and add noise without helping
# the user understand progress. They still land in the DB, just marked
# as "debug" so the frontend can hide them.
_NOISE_PATTERNS = (
    re.compile(r"^Cbc\d+I\s"),               # CBC internal status lines
    re.compile(r"^Cgl\d+I\s"),               # CGL cut generator status
    re.compile(r"was tried \d+ times and created \d+ cuts"),
    re.compile(r"^(Gomory|Probing|Knapsack|Clique|MixedIntegerRounding2|"
               r"FlowCover|TwoMirCuts|ZeroHalf)\s"),
    re.compile(r"^(Time|Total time|Total iterations|Enumerated nodes|"
               r"Lower bound|Gap|Objective value)[: ]"),
    re.compile(r"^(Option for|threads was|seconds was|ratioGap was|"
               r"verbose was|command line)\b"),
    re.compile(r"^(Version|Build Date|Continuous objective value)\b"),
    re.compile(r"^Welcome to the CBC"),
)


def _sanitise_line(raw: str) -> str:
    """Strip ANSI + control chars and collapse whitespace."""
    text = _ANSI_RE.sub("", raw)
    text = _CTRL_RE.sub("", text)
    return text.strip()


def _classify_line(text: str) -> str:
    """Return 'debug' for noisy solver bookkeeping, 'info' otherwise."""
    for pat in _NOISE_PATTERNS:
        if pat.search(text):
            return "debug"
    return "info"

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
        # Accumulates partial PTY reads until we see a newline so we
        # never split a word across two log rows.
        self._pty_buffer = ""

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

        # Pull name + description out of params so they can live in real
        # columns (label / description). They're still kept inside params
        # so the R script and resolved_config see the full context.
        label = (params.get("name") or "").strip() or None
        description = (params.get("description") or "").strip() or None

        # === create run ===
        row = await self.pg.execute(
            """
            INSERT INTO bioprotect.prioritizr_runs
                (project_id, status, params, label, description)
            VALUES (%s, 'queued', %s::jsonb, %s, %s)
            RETURNING id
            """,
            data=[project_id, json.dumps(params or {}), label, description],
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

        try:
            await self.pg.execute(
                "SELECT bioprotect.prepare_prioritizr_input(%s)",
                data=[self.run_id],
            )
        except Exception as e:
            await self.pg.execute(
                "UPDATE bioprotect.prioritizr_runs SET status='Failed', error=%s WHERE id=%s",
                data=[str(e), self.run_id],
            )
            self.close({
                "status": "Failed",
                "run_id": self.run_id,
                "error": str(e),
                "info": f"Failed to prepare input: {e}",
            })
            return

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
                    SET status='Failed', error=%s
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
                "UPDATE bioprotect.prioritizr_runs SET status='Finished' WHERE id=%s",
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
                SET status='Failed', error=%s
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
        # Flush any trailing content that never got a newline
        if self._pty_buffer:
            await self._emit_line(self._pty_buffer)
            self._pty_buffer = ""

    async def _ingest_text(self, text: str):
        """
        Buffer partial lines and emit only complete ones.

        PTY reads arrive in arbitrary chunks, so we glue them onto the
        running buffer and split on newlines. Anything after the final
        newline stays in the buffer until more data arrives.
        """
        self._pty_buffer += text
        # Normalise CR-only / CRLF line endings so splitlines works
        # cleanly regardless of what R flushed.
        while True:
            # Prefer explicit line breaks; also treat a bare \r as a
            # "refresh current line" and flush what we have so far.
            newline_idx = -1
            for i, ch in enumerate(self._pty_buffer):
                if ch in ("\n", "\r"):
                    newline_idx = i
                    break
            if newline_idx < 0:
                break
            line = self._pty_buffer[:newline_idx]
            # Skip the terminator plus any paired \n after \r
            end = newline_idx + 1
            if (
                self._pty_buffer[newline_idx] == "\r"
                and end < len(self._pty_buffer)
                and self._pty_buffer[end] == "\n"
            ):
                end += 1
            self._pty_buffer = self._pty_buffer[end:]
            await self._emit_line(line)

    async def _emit_line(self, raw_line: str):
        """Sanitise a single line and persist/forward it if non-empty."""
        clean = _sanitise_line(raw_line)
        if not clean:
            return
        level = _classify_line(clean)

        # Persist every sanitised line to the DB so the full run log is
        # still available for debugging, with a level tag.
        await self.pg.execute(
            """
            INSERT INTO bioprotect.prioritizr_run_logs (run_id, stream, message)
            VALUES (%s, %s, %s)
            """,
            data=[self.run_id, level, clean],
        )

        # Only forward "info" lines over the websocket — that's all the
        # user needs to see scroll past. The noisy solver bookkeeping
        # stays queryable in the DB via the "debug" stream.
        if level == "info":
            self.send_response({
                "status": "Running",
                "stream": "pty",
                "level": level,
                "message": clean,
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
