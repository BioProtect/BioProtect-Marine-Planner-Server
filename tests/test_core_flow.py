"""The main user path through the real handlers: create a planning grid over
the WebSocket, create a project on it, pick a cost profile, start a
Prioritizr run. Handlers commit on their own pool, so fixtures delete what
they made. The R solve itself is in test_prioritizr_run.py (slow).
"""

import json
import os
import uuid

import h3
import pytest

from conftest import (AREA, api, create_grid, create_project, drop_grid,
                      import_folder, rows, shapefile_zip, ws_messages)

# --- planning grid ----------------------------------------------------------

async def test_grid_from_shapefile_writes_cells_metadata_and_view(server, grid):
    alias, pu_id, cells = grid
    got = await rows(server.pg, "SELECT h3_index, resolution FROM bioprotect.h3_cells "
                                "WHERE project_area = %s", [alias])
    assert {r["h3_index"] for r in got} == set(cells) and len(cells) > 5
    assert {r["resolution"] for r in got} == {7}
    meta = (await rows(server.pg, "SELECT feature_class_name, planning_unit_count "
                                  "FROM bioprotect.metadata_planning_units WHERE unique_id = %s", [pu_id]))[0]
    assert meta["planning_unit_count"] == len(cells)
    view = await rows(server.pg, f'SELECT count(*) AS n FROM bioprotect."{meta["feature_class_name"]}"')
    assert view[0]["n"] == len(cells)
    # the upload is removed from imports/ on success
    assert not [f for f in os.listdir(import_folder()) if f.startswith("pytest_grid_")]


async def test_same_grid_again_reuses_it(server, grid):
    alias, pu_id, _ = grid
    messages = await create_grid(server, alias, shapefile_zip(import_folder()))
    assert messages[-1]["type"] == "grid_exists"
    assert messages[-1]["planning_unit_id"] == pu_id


async def test_grid_rejects_bad_resolution_with_an_error_message(server):
    import asyncio
    messages = await asyncio.wait_for(
        create_grid(server, "Pytest never", "/nonexistent.zip", resolution=5), 5)
    assert "Invalid resolution" in messages[-1].get("error", "")


# --- project ----------------------------------------------------------------

async def test_project_gets_grid_cells_edges_owner_and_feature_targets(server, grid):
    alias, pu_id, cells = grid
    fid = (await rows(server.pg, "INSERT INTO bioprotect.metadata_interest_features "
                                 "(feature_class_name, alias) VALUES (%s, %s) RETURNING unique_id",
                      [f"pytest_f_{uuid.uuid4().hex[:6]}"] * 2))[0]["unique_id"]
    try:
        pid = await create_project(server, alias, interest_features=[fid],
                                   target_values=["40"], spf_values=["2"])

        pus = await rows(server.pg, "SELECT h3_index FROM bioprotect.project_pus WHERE project_id = %s", [pid])
        assert {r["h3_index"] for r in pus} == set(cells)
        proj = (await rows(server.pg, "SELECT planning_unit_id, default_resolution "
                                      "FROM bioprotect.projects WHERE id = %s", [pid]))[0]
        assert proj == {"planning_unit_id": pu_id, "default_resolution": 7}
        owner = await rows(server.pg, "SELECT user_id, role FROM bioprotect.user_projects WHERE project_id = %s", [pid])
        assert owner == [{"user_id": server.user_id["pytest_admin"], "role": "owner"}]
        edges = await rows(server.pg, "SELECT h3_a, h3_b FROM bioprotect.grid_boundary_edges "
                                      "WHERE planning_unit_id = %s", [pu_id])
        expected = {tuple(sorted((a, b))) for a in cells for b in h3.grid_ring(a, 1) if b in cells}
        assert {(e["h3_a"], e["h3_b"]) for e in edges} == expected
        pf = await rows(server.pg, "SELECT target_value, spf FROM bioprotect.project_features "
                                   "WHERE project_id = %s AND feature_unique_id = %s", [pid, fid])
        assert [(float(r["target_value"]), float(r["spf"])) for r in pf] == [(40.0, 2.0)]
    finally:
        await server.pg.execute("DELETE FROM bioprotect.metadata_interest_features WHERE unique_id = %s", [fid])


async def test_project_on_grid_whose_name_has_brackets(server):
    alias = f"Pytest Bay (North {uuid.uuid4().hex[:6]})"
    try:
        messages = await create_grid(server, alias, shapefile_zip(import_folder()))
        assert messages[-1].get("success"), messages
        pid = await create_project(server, alias)
        pus = await rows(server.pg, "SELECT 1 FROM bioprotect.project_pus WHERE project_id = %s", [pid])
        assert len(pus) > 0
    finally:
        await drop_grid(server.pg, alias)


async def test_project_on_unknown_grid_is_an_error(server):
    code, body = await api(server, "POST", "/server/projects", {"action": "create"},
                           {"project": "pytest x", "planning_grid_name": "no such grid (Res 7)"})
    assert body == {"error": "Planning grid not found"}


async def test_project_on_a_grid_with_no_cells_is_refused_and_not_kept(server):
    alias = f"pytest ghost {uuid.uuid4().hex[:6]}"
    await server.pg.execute(
        "INSERT INTO bioprotect.metadata_planning_units (feature_class_name, alias) "
        "VALUES (%s, %s)", [f"v_h3_{alias.replace(' ', '_')}_res7", f"{alias} (Res 7)"])
    try:
        code, body = await api(server, "POST", "/server/projects", {"action": "create"},
                               {"project": alias, "planning_grid_name": f"{alias} (Res 7)"})
        assert body == {"error": "Planning grid has no cells at resolution 7"}
        assert await rows(server.pg, "SELECT 1 FROM bioprotect.projects WHERE name = %s", [alias]) == []
    finally:
        await server.pg.execute("DELETE FROM bioprotect.metadata_planning_units WHERE alias = %s",
                                [f"{alias} (Res 7)"])


async def test_owner_deletes_project(server, grid):
    pid = await create_project(server, grid[0])
    code, body = await api(server, "POST", "/server/projects", {"action": "delete"}, {"project_id": pid})
    assert body == {"info": "Project deleted"}
    assert await rows(server.pg, "SELECT 1 FROM bioprotect.projects WHERE id = %s", [pid]) == []


# --- cost profiles ----------------------------------------------------------

async def _profile(pg, pid, name="pytest"):
    return (await rows(pg, "INSERT INTO bioprotect.cost_profiles (project_id, name) "
                           "VALUES (%s, %s) RETURNING id", [pid, name]))[0]["id"]


async def test_set_active_profile_and_delete_rules(server, grid):
    pid = await create_project(server, grid[0])
    a, b = await _profile(server.pg, pid, "a"), await _profile(server.pg, pid, "b")

    _, body = await api(server, "GET", "/server/setActiveCostProfile",
                        {"project_id": pid, "cost_profile_id": a})
    assert body == {"info": "Cost profile activated"}
    assert (await rows(server.pg, "SELECT active_cost_profile_id AS id FROM bioprotect.projects "
                                  "WHERE id = %s", [pid]))[0]["id"] == a

    # the active one can't be deleted, the other can
    _, body = await api(server, "GET", "/server/deleteCost", {"cost_profile_id": a})
    assert "currently active" in body["error"]
    _, body = await api(server, "GET", "/server/deleteCost", {"cost_profile_id": b})
    assert body == {"info": "Cost profile deleted"}


async def test_profile_of_another_project_cannot_be_activated(server, grid):
    p1, p2 = await create_project(server, grid[0]), await create_project(server, grid[0])
    other = await _profile(server.pg, p2)
    _, body = await api(server, "GET", "/server/setActiveCostProfile",
                        {"project_id": p1, "cost_profile_id": other})
    assert "not found for project" in body["error"]


# --- Prioritizr run start ---------------------------------------------------

async def test_run_without_cost_profile_fails_cleanly_before_r(server, grid):
    """A new project has no active cost profile, so input prep refuses it.
    The run must end Failed with the reason, not hang or stay 'preparing'."""
    pid = await create_project(server, grid[0])
    messages, code = await ws_messages(server, "/server/prioritizr-ws",
                                       {"user": server.user_id["pytest_admin"], "project_id": pid,
                                        "params": json.dumps({"name": "pytest"})})
    final = messages[-1]
    # SocketHandler.close() always sets status 'Finished'; the client keys on 'error'
    assert "no active_cost_profile_id" in final["error"] and code == 1000
    run = (await rows(server.pg, "SELECT status, error, label FROM bioprotect.prioritizr_runs "
                                 "WHERE id = %s", [final["run_id"]]))[0]
    assert run["status"] == "Failed" and "no active_cost_profile_id" in run["error"]
    assert run["label"] == "pytest"
