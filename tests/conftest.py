"""Shared test setup. Runs before any app module is imported.

Points the app at a throwaway `bioprotect_test` database and a temp
imports/exports folder, so the dev DB and server/imports are never touched.
The DB is rebuilt from db/install + `migrate.py deploy` once per pytest run,
and only if some test asks for it (the `test_db` / `db` fixtures).
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile
import uuid
import zipfile

import psycopg2
import pytest
from dotenv import dotenv_values
from shapely.geometry import box

SERVER = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_DB = "bioprotect_test"

# The app reads .env.local and ./data/... relative to the working directory.
os.chdir(SERVER)

_env = dotenv_values(".env.local")
_tmp = tempfile.mkdtemp(prefix="bioprotect_tests_")
os.makedirs(os.path.join(_tmp, "imports"))
os.makedirs(os.path.join(_tmp, "exports"))

# BIOPROTECT_* overrides .env.local in the app (classes/db_config.py);
# PG* is what migrate.py, psql and the R script read.
os.environ.update({
    "BIOPROTECT_DB_NAME": TEST_DB,
    "BIOPROTECT_TEST_IMPORT_DIR": os.path.join(_tmp, "imports") + os.sep,
    "BIOPROTECT_TEST_EXPORT_DIR": os.path.join(_tmp, "exports") + os.sep,
    "PGHOST": _env["db_host"],
    "PGUSER": _env["db_user"],
    "PGPASSWORD": _env["db_pass"],
    "PGDATABASE": TEST_DB,
})


def connect(dbname=TEST_DB):
    return psycopg2.connect(dbname=dbname, host=_env["db_host"],
                            user=_env["db_user"], password=_env["db_pass"])


@pytest.fixture(scope="session")
def test_db():
    """Drop and rebuild bioprotect_test (about 2s)."""
    admin = connect("postgres")
    admin.autocommit = True
    with admin.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS {TEST_DB} WITH (FORCE)")
        cur.execute(f"CREATE DATABASE {TEST_DB}")
    admin.close()
    # psql, not psycopg2: the pg_dump output uses psql meta-commands (\restrict)
    subprocess.run(["psql", "-q", "-X", "-v", "ON_ERROR_STOP=1", "-d", TEST_DB,
                    "-f", "db/install/01_schema.sql",
                    "-f", "db/install/02_seed.sql"],
                   check=True, stdout=subprocess.DEVNULL)
    subprocess.run([sys.executable, "db/migrate.py", "deploy"],
                   check=True, stdout=subprocess.DEVNULL)
    # Two real users: user_projects has an FK to users, and the auth tests
    # need a non-Admin. Both log in with PASSWORD.
    from passlib.hash import bcrypt
    conn = connect()
    with conn, conn.cursor() as cur:
        for username, role in USERS.items():
            cur.execute("INSERT INTO bioprotect.users (username, password_hash, role) "
                        "VALUES (%s, %s, %s)", [username, bcrypt.hash(PASSWORD), role])
    conn.close()
    return TEST_DB


USERS = {"pytest_admin": "Admin", "pytest_user": "User"}
PASSWORD = "pytest-password"


@pytest.fixture
def db(test_db):
    """A cursor on the test DB; everything it does is rolled back afterwards.
    Only for SQL run directly by the test. App code commits on its own pool,
    so tests of handlers/services must clean up what they create."""
    conn = connect()
    yield conn.cursor()
    conn.rollback()
    conn.close()


@pytest.fixture
async def server(test_db):
    """The real Application on a free port, against the test DB, plus signed
    login cookies. Mirrors initialiseApp() minus logging/listen.
    Yields .host ("127.0.0.1:port"), .headers (pytest_admin's cookies),
    .headers_for(username) (anyone's, {} for None) and .pg."""
    from types import SimpleNamespace

    from tornado.httpserver import HTTPServer
    from tornado.testing import bind_unused_port
    from tornado.web import create_signed_value

    import app
    from classes import postgis_class
    from classes.db_config import DBConfig
    from classes.folder_path_config import get_folder_path_config

    postgis_class.pg = None  # the pool is bound to an event loop: new one per test
    app.project_paths = get_folder_path_config()
    app.db_config = DBConfig()
    await app.set_global_vars()
    srv = HTTPServer(app.Application())
    sock, port = bind_unused_port()
    srv.add_sockets([sock])

    secret = app.db_config.COOKIE_SECRET
    rows = await app.pg.execute("SELECT id, username, role FROM bioprotect.users",
                                return_format="Array")
    ids = {r["username"]: (r["id"], r["role"]) for r in rows}

    def headers_for(username):
        """Signed cookies as AuthHandler sets them at login."""
        if username is None:
            return {}
        uid, role = ids[username]
        cookies = {"user": username, "role": role, "user_id": str(uid)}
        return {"Cookie": "; ".join(f"{k}={create_signed_value(secret, k, v).decode()}"
                                    for k, v in cookies.items())}

    yield SimpleNamespace(host=f"127.0.0.1:{port}", headers=headers_for("pytest_admin"),
                          headers_for=headers_for,
                          user_id={u: v[0] for u, v in ids.items()}, pg=app.pg)
    srv.stop()
    await app.pg.close_pool()
    postgis_class.pg = None
    # Tornado's Subprocess binds its SIGCHLD handler to the first event loop
    # that uses it; without this, ogr2ogr's exit is never seen in later tests
    # (each gets a new loop) and run_command hangs.
    from tornado.process import Subprocess
    Subprocess.uninitialize()


async def ws_messages(server, path, params, send=None, headers=None, until=None):
    """Open a WebSocket on the test server, optionally send one JSON message,
    and collect messages until the server closes it - or until `until(msg)`
    is true, for handlers that never close (the grid WebSocket).
    Returns (messages, close_code)."""
    import json
    from urllib.parse import urlencode

    from tornado.httpclient import HTTPRequest
    from tornado.websocket import websocket_connect

    conn = await websocket_connect(HTTPRequest(
        f"ws://{server.host}{path}?{urlencode(params)}",
        headers=server.headers if headers is None else headers))
    if send is not None:
        await conn.write_message(json.dumps(send))
    messages = []
    while (raw := await conn.read_message()) is not None:
        messages.append(json.loads(raw))
        if until and until(messages[-1]):
            conn.close()
            break
    return messages, conn.close_code


def pytest_sessionfinish(session, exitstatus):
    shutil.rmtree(_tmp, ignore_errors=True)


# --- grid / project helpers shared by the handler tests -------------------

# ~0.1° square off Galway: a dozen or so res-7 cells, no land handling needed
AREA = box(-9.6, 53.0, -9.5, 53.1)


async def api(server, method, path, params=None, body=None, user="pytest_admin"):
    """Call a REST endpoint; errors come back as 200 + {"error": ...}.
    Returns (status code, parsed JSON body)."""
    from urllib.parse import urlencode
    from tornado.httpclient import AsyncHTTPClient
    url = f"http://{server.host}{path}" + (f"?{urlencode(params)}" if params else "")
    resp = await AsyncHTTPClient().fetch(
        url, method=method, headers=server.headers_for(user), raise_error=False,
        body=json.dumps(body) if method == "POST" else None)
    try:
        parsed = json.loads(resp.body) if resp.body else None
    except ValueError:
        parsed = resp.body.decode()
    return resp.code, parsed


def shapefile_zip(folder, geom=None):
    """A zipped one-polygon shapefile written to folder; the .shp parts are
    built in a temp dir so only the zip lands in folder."""
    import geopandas as gpd
    name = f"pytest_grid_{uuid.uuid4().hex[:8]}"
    shp_dir = tempfile.mkdtemp()
    gpd.GeoDataFrame({"id": [1]}, geometry=[geom or AREA], crs=4326).to_file(
        os.path.join(shp_dir, f"{name}.shp"))
    zip_path = os.path.join(folder, f"{name}.zip")
    with zipfile.ZipFile(zip_path, "w") as z:
        for f in os.listdir(shp_dir):
            z.write(os.path.join(shp_dir, f), f)
    shutil.rmtree(shp_dir)
    return zip_path


async def create_grid(server, alias, zip_path, resolution=7):
    """Returns the messages up to the first success / error / grid_exists."""
    messages, _ = await ws_messages(
        server, "/server/createPlanningUnitGrid", {}, until=lambda m: (
            m.get("success") or "error" in m or m.get("type") == "grid_exists"),
        send={"shapefile_path": zip_path, "alias": alias,
              "description": "pytest", "resolution": resolution})
    return messages


async def drop_grid(pg, alias, resolution=7):
    rows = await pg.execute("SELECT unique_id, feature_class_name FROM bioprotect.metadata_planning_units "
                            "WHERE alias = %s", [f"{alias} (Res {resolution})"], return_format="Array")
    for r in rows:
        await pg.execute("DELETE FROM bioprotect.prioritizr_runs WHERE project_id IN "
                         "(SELECT id FROM bioprotect.projects WHERE planning_unit_id = %s)", [r["unique_id"]])
        await pg.execute("DELETE FROM bioprotect.projects WHERE planning_unit_id = %s", [r["unique_id"]])
        await pg.execute("DELETE FROM bioprotect.grid_boundary_edges WHERE planning_unit_id = %s", [r["unique_id"]])
        await pg.execute("DELETE FROM bioprotect.metadata_planning_units WHERE unique_id = %s", [r["unique_id"]])
        await pg.execute(f'DROP MATERIALIZED VIEW IF EXISTS bioprotect."{r["feature_class_name"]}"')
    await pg.execute("DELETE FROM bioprotect.h3_cells WHERE project_area = %s", [alias])


def import_folder():
    from classes.folder_path_config import get_folder_path_config
    return get_folder_path_config().IMPORT_FOLDER


@pytest.fixture
async def grid(server):
    """A fresh res-7 grid over AREA. Yields (alias, planning_unit_id, cells)."""
    alias = f"Pytest Bay {uuid.uuid4().hex[:6]}"
    messages = await create_grid(server, alias, shapefile_zip(import_folder()))
    done = messages[-1]
    assert done.get("success"), messages
    import h3
    yield alias, done["planning_unit_id"], h3.geo_to_cells(AREA.__geo_interface__, 7)
    await drop_grid(server.pg, alias)


async def create_project(server, grid_alias, **extra):
    code, body = await api(server, "POST", "/server/projects", {"action": "create"}, {
        "project": f"pytest project {uuid.uuid4().hex[:6]}", "description": "pytest",
        "planning_grid_name": f"{grid_alias} (Res 7)", **extra})
    assert code == 200 and "error" not in body, body
    return body["project_id"]


async def rows(pg, sql, args=()):
    return await pg.execute(sql, list(args), return_format="Array")
