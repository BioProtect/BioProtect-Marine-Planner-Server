"""A job longer than the ping timeout must not get the socket cut off.
Tornado reads no frames (pongs) until open() returns; SocketHandler runs the
job as a task so pongs keep arriving and the close is a clean 1000."""
import asyncio

from tornado.httpserver import HTTPServer
from tornado.testing import bind_unused_port
from tornado.web import Application
from tornado.websocket import websocket_connect

from handlers.websocket_handler import SocketHandler


class _Slow(SocketHandler):
    def prepare(self):  # no auth cookie in this test
        pass

    async def run(self):
        for i in range(4):
            await asyncio.sleep(1)
            self.write_message(f"tick {i}")
        self.close(clean=False)


def test_long_job_survives_ping_timeout():
    async def go():
        sock, port = bind_unused_port()
        app = Application([("/ws", _Slow)], websocket_ping_interval=0.5,
                          websocket_ping_timeout=1)
        server = HTTPServer(app)
        server.add_sockets([sock])
        c = await websocket_connect(f"ws://127.0.0.1:{port}/ws")
        got = []
        while (m := await c.read_message()) is not None:
            got.append(m)
        server.stop()
        return got, c.close_code

    got, code = asyncio.run(go())
    assert got == [f"tick {i}" for i in range(4)], got
    assert code == 1000, code
