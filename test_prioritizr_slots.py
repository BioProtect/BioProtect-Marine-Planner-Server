"""Prioritizr run cap: at most MAX_CONCURRENT_RUNS solve at once, the rest
wait, and a run whose socket closed while waiting never starts.
No DB or R needed - both are stubbed. Run: python test_prioritizr_slots.py"""
import asyncio
import types

import handlers.prioritizr_websocket_handler as m
from handlers.websocket_handler import SocketHandler


async def _noop_open(self, msg):
    pass


class _PG:
    async def execute(self, *a, **k):
        return [{"id": 1}]


def main():
    SocketHandler.open = _noop_open
    state = {"active": 0, "peak": 0, "started": [], "msgs": []}

    def build(i, closed=False):
        h = m.PrioritizrWSHandler.__new__(m.PrioritizrWSHandler)
        h.request = types.SimpleNamespace(arguments={})
        h.pg, h.proc, h.master_fd, h.run_id = _PG(), None, None, None
        h._closed, h._pty_buffer = closed, ""
        h.validate_args = lambda *a: None
        h.get_argument = lambda n, d=None: {"project_id": "1", "user": "1"}.get(n, d)
        h.get_secure_cookie = lambda n: None
        h.send_response = lambda msg: state["msgs"].append(msg.get("info") or "")

        async def fake_run():
            state["started"].append(i)
            state["active"] += 1
            state["peak"] = max(state["peak"], state["active"])
            await asyncio.sleep(0.05)
            state["active"] -= 1
        h._prepare_and_run = fake_run
        return h

    async def run():
        hs = [build(i, closed=(i == 5)) for i in range(6)]
        await asyncio.gather(*(h.open() for h in hs))

    asyncio.run(run())
    assert state["peak"] == m.MAX_CONCURRENT_RUNS == 4, state["peak"]
    assert sum("already in progress" in x for x in state["msgs"]) == 2
    assert 5 not in state["started"]
    print("all prioritizr slot checks passed")


if __name__ == "__main__":
    main()
