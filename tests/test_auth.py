"""Login, logout and who may touch whose project.

Tests marked xfail(strict=True) assert the behaviour we want and currently
fail because of a real hole (see each reason). Fix the hole and the test
starts passing, which strict mode reports as a failure: remove the marker.
"""

import json

import pytest
from tornado.httpclient import AsyncHTTPClient, HTTPClientError, HTTPRequest
from tornado.websocket import websocket_connect

from conftest import PASSWORD, api, create_project, rows, ws_messages


async def login(server, username, password):
    resp = await AsyncHTTPClient().fetch(
        f"http://{server.host}/server/auth", method="POST", raise_error=False,
        body=json.dumps({"username": username, "password": password}))
    return resp


def cookie_names(resp):
    return {c.split("=", 1)[0] for c in resp.headers.get_list("Set-Cookie")}


# --- login / logout ---------------------------------------------------------

async def test_login_sets_signed_session_cookies_and_hides_the_hash(server):
    resp = await login(server, "pytest_user", PASSWORD)
    assert resp.code == 200
    assert {"user", "user_id", "role", "jwt"} <= cookie_names(resp)
    assert b"password_hash" not in resp.body and b"refresh_tokens" not in resp.body


@pytest.mark.parametrize("username, password, code", [
    ("pytest_user", "wrong", 401),
    ("nobody", PASSWORD, 401),
    ("pytest_user", "", 400),
])
async def test_login_refused(server, username, password, code):
    resp = await login(server, username, password)
    assert resp.code == code
    assert not {"user", "user_id", "role"} & cookie_names(resp)


async def test_logout_clears_the_session_cookies(server):
    resp = await AsyncHTTPClient().fetch(
        f"http://{server.host}/server/users?action=logout",
        headers=server.headers_for("pytest_user"))
    cleared = [c for c in resp.headers.get_list("Set-Cookie") if "expires=" in c.lower()]
    assert {c.split("=", 1)[0] for c in cleared} >= {"user", "user_id", "role", "jwt"}


# --- not logged in ----------------------------------------------------------

async def test_anonymous_project_list_is_refused(server):
    _, body = await api(server, "GET", "/server/projects", {"action": "list"}, user=None)
    assert body == {"error": "Not authenticated"}


async def test_anonymous_websocket_is_refused(server):
    with pytest.raises(HTTPClientError) as e:
        await websocket_connect(HTTPRequest(f"ws://{server.host}/server/importFeatures"))
    assert e.value.code == 403


# --- other people's projects ------------------------------------------------

async def test_project_list_only_shows_your_own(server, grid):
    pid = await create_project(server, grid[0])  # owned by pytest_admin
    _, mine = await api(server, "GET", "/server/projects", {"action": "list"})
    _, theirs = await api(server, "GET", "/server/projects", {"action": "list"}, user="pytest_user")
    assert pid in {p["id"] for p in mine["projects"]}
    assert pid not in {p["id"] for p in theirs["projects"]}


@pytest.mark.parametrize("method, action, body", [
    ("GET", "get", None),
    ("POST", "rename", {"newName": "hijacked"}),
    ("POST", "delete", {}),
])
async def test_non_member_cannot_read_rename_or_delete(server, grid, method, action, body):
    pid = await create_project(server, grid[0])
    params = {"action": action, "projectId": pid, "project_id": pid}
    _, resp = await api(server, method, "/server/projects", params,
                        {**body, "project_id": pid} if body is not None else None,
                        user="pytest_user")
    assert resp == {"error": "Access denied"}
    left = await rows(server.pg, "SELECT name FROM bioprotect.projects WHERE id = %s", [pid])
    assert left and left[0]["name"] != "hijacked"


async def test_only_starter_owner_or_admin_deletes_a_run(server, grid):
    pid = await create_project(server, grid[0])
    run = (await rows(server.pg, "INSERT INTO bioprotect.prioritizr_runs (project_id, created_by) "
                                 "VALUES (%s, %s) RETURNING id",
                      [pid, server.user_id["pytest_admin"]]))[0]["id"]
    code, _ = await api(server, "POST", "/server/prioritizr",
                        {"action": "delete-run", "run-id": run}, {}, user="pytest_user")
    assert code == 403
    _, body = await api(server, "POST", "/server/prioritizr", {"action": "delete-run", "run-id": run}, {})
    assert body["info"] == f"Run {run} deleted"


# --- access control (were holes, fixed 2026-11-06) ---------------------

async def test_anonymous_cannot_make_a_user_admin(server):
    uid = server.user_id["pytest_user"]
    try:
        await api(server, "POST", "/server/users",
                  {"action": "update", "user_id": uid, "role": "Admin"}, {}, user=None)
        role = (await rows(server.pg, "SELECT role FROM bioprotect.users WHERE id = %s", [uid]))[0]["role"]
        assert role == "User"
    finally:
        await server.pg.execute("UPDATE bioprotect.users SET role = 'User' WHERE id = %s", [uid])


async def test_user_cannot_change_someone_elses_password(server):
    from passlib.hash import bcrypt
    admin = server.user_id["pytest_admin"]
    try:
        await api(server, "POST", "/server/users",
                  {"action": "update", "user_id": admin, "password": "taken"}, {}, user="pytest_user")
        h = (await rows(server.pg, "SELECT password_hash FROM bioprotect.users WHERE id = %s", [admin]))[0]
        assert bcrypt.verify(PASSWORD, h["password_hash"])
    finally:
        await server.pg.execute("UPDATE bioprotect.users SET password_hash = %s WHERE id = %s",
                                [bcrypt.hash(PASSWORD), admin])


async def test_non_member_cannot_change_or_delete_cost_profiles(server, grid):
    pid = await create_project(server, grid[0])
    cp = (await rows(server.pg, "INSERT INTO bioprotect.cost_profiles (project_id, name) "
                                "VALUES (%s, 'pytest') RETURNING id", [pid]))[0]["id"]
    await api(server, "GET", "/server/setActiveCostProfile",
              {"project_id": pid, "cost_profile_id": cp}, user=None)
    active = await rows(server.pg, "SELECT active_cost_profile_id AS id FROM bioprotect.projects "
                                   "WHERE id = %s", [pid])
    assert active[0]["id"] is None


async def test_non_member_cannot_start_a_run(server, grid):
    pid = await create_project(server, grid[0])
    await ws_messages(server, "/server/prioritizr-ws",
                      {"user": server.user_id["pytest_user"], "project_id": pid},
                      headers=server.headers_for("pytest_user"))
    assert await rows(server.pg, "SELECT 1 FROM bioprotect.prioritizr_runs WHERE project_id = %s", [pid]) == []


# --- user create / self-rename ----------------------------------------------

async def test_only_an_admin_creates_users_and_they_default_to_user(server):
    import uuid
    name = f"pytest_new_{uuid.uuid4().hex[:6]}"
    new = {"username": name, "email": f"{name}@x.test", "password": "pw"}
    try:
        for caller in (None, "pytest_user"):
            await api(server, "POST", "/server/users", {"action": "create"}, new, user=caller)
            assert await rows(server.pg, "SELECT 1 FROM bioprotect.users WHERE username = %s", [name]) == []
        code, _ = await api(server, "POST", "/server/users", {"action": "create"}, new)
        assert code == 201
        made = await rows(server.pg, "SELECT role FROM bioprotect.users WHERE username = %s", [name])
        assert made == [{"role": "User"}]
    finally:
        await server.pg.execute("DELETE FROM bioprotect.users WHERE username = %s", [name])


async def test_self_rename_via_name_refreshes_the_user_cookie(server):
    from urllib.parse import urlencode
    uid = server.user_id["pytest_user"]
    try:
        resp = await AsyncHTTPClient().fetch(
            f"http://{server.host}/server/users?" + urlencode(
                {"action": "update", "name": "pytest_user_renamed"}),
            method="POST", body="{}", headers=server.headers_for("pytest_user"))
        assert "user" in cookie_names(resp)
        got = await rows(server.pg, "SELECT username FROM bioprotect.users WHERE id = %s", [uid])
        assert got == [{"username": "pytest_user_renamed"}]
    finally:
        await server.pg.execute("UPDATE bioprotect.users SET username = 'pytest_user' WHERE id = %s", [uid])
