"""Full Prioritizr runs through the WebSocket: real R, real HiGHS/CBC.
Slow, so left out of the default run: `pytest -m slow`.

The problem is built so the optimum is known: every cell holds 1 unit of
one feature, west cells cost 1 and east cells cost 10, one east cell is
locked in and one west cell locked out.
"""

import json
import math
import uuid

import pytest

from conftest import create_project, rows, ws_messages
from test_costs import side

pytestmark = pytest.mark.slow


@pytest.fixture
async def problem(server, grid):
    alias, _, cells = grid
    cells = [c for c in cells if side(c)]  # only cells clearly west or east
    pg = server.pg
    pid = await create_project(server, alias)
    name = f"pytest_f_{uuid.uuid4().hex[:6]}"
    fid = (await rows(pg, "INSERT INTO bioprotect.metadata_interest_features "
                          "(feature_class_name, alias) VALUES (%s, %s) RETURNING unique_id",
                      [name, name]))[0]["unique_id"]
    await pg.execute("INSERT INTO bioprotect.project_features (project_id, feature_unique_id, target_value) "
                     "VALUES (%s, %s, 40)", [pid, fid])
    # cells on SPLIT are dropped from the project so every PU is one or the other
    await pg.execute("DELETE FROM bioprotect.project_pus WHERE project_id = %s AND NOT h3_index = ANY(%s)",
                     [pid, cells])
    west = sorted(c for c in cells if side(c) == "west")
    east = sorted(c for c in cells if side(c) == "east")
    locked_in, locked_out = east[0], west[0]
    cp = (await rows(pg, "INSERT INTO bioprotect.cost_profiles (project_id, name) "
                         "VALUES (%s, 'pytest') RETURNING id", [pid]))[0]["id"]
    await pg.execute(
        "INSERT INTO bioprotect.cost_profile_values (cost_profile_id, project_pu_id, cost, status) "
        "SELECT %s, pp.id, CASE WHEN pp.h3_index = ANY(%s) THEN 1 ELSE 10 END, "
        "       CASE pp.h3_index WHEN %s THEN 1 WHEN %s THEN 2 ELSE 0 END "
        "  FROM bioprotect.project_pus pp WHERE pp.project_id = %s",
        [cp, west, locked_in, locked_out, pid])
    await pg.execute("UPDATE bioprotect.projects SET active_cost_profile_id = %s WHERE id = %s", [cp, pid])
    for c in cells:
        await pg.execute("INSERT INTO bioprotect.pu_feature_amounts VALUES (%s, %s, %s, 1)", [pid, fid, c])
    yield dict(pid=pid, west=west, east=east, locked_in=locked_in, locked_out=locked_out)
    await pg.execute("DELETE FROM bioprotect.metadata_interest_features WHERE unique_id = %s", [fid])


async def run(server, pid, params, log=None):
    messages, _ = await ws_messages(server, "/server/prioritizr-ws", {
        "user": server.user_id["pytest_admin"], "project_id": pid,
        "params": json.dumps({"name": "pytest run", **params})})
    final = messages[-1]
    assert "error" not in final, (final, [m.get("info") for m in messages[-15:]])
    if log is not None:
        log.extend(str(m.get("info") or m.get("message") or "") for m in messages)
    status = (await rows(server.pg, "SELECT status FROM bioprotect.prioritizr_runs WHERE id = %s",
                         [final["run_id"]]))[0]["status"]
    assert status == "Finished"
    got = await rows(server.pg, "SELECT h3_index, solution FROM bioprotect.prioritizr_run_results "
                                "WHERE run_id = %s", [final["run_id"]])
    return {r["h3_index"]: r["solution"] for r in got}


async def test_min_set_meets_target_at_least_cost_and_respects_locks(server, problem):
    p = problem
    n = len(p["west"]) + len(p["east"])
    need = math.ceil(0.4 * n)
    assert len(p["west"]) - 1 >= need - 1, "grid too lopsided for a known optimum"

    sol = await run(server, p["pid"], {"mode": "species", "solver": {"gap": 0}})

    assert set(sol) == set(p["west"]) | set(p["east"])  # one row per PU
    chosen = {c for c, s in sol.items() if s == 1}
    assert p["locked_in"] in chosen and p["locked_out"] not in chosen
    assert len(chosen) == need  # 1 unit per cell, so exactly the target
    assert chosen & set(p["east"]) == {p["locked_in"]}  # nothing expensive beyond the lock


async def test_area_mode_stays_within_the_area_budget(server, problem):
    p = problem
    sol = await run(server, p["pid"], {"mode": "area", "targets": {"prop": 0.3}})
    chosen = [c for c, s in sol.items() if s == 1]
    # all res-7 cells here are near-equal in area, so the budget is a cell count
    assert 0 < len(chosen) <= math.floor(0.3 * len(sol)) + 1
    assert p["locked_in"] in chosen and p["locked_out"] not in chosen


@pytest.mark.parametrize("mode", ["species", "area"])
async def test_clustering_rounds_keep_targets_and_locks(server, problem, mode):
    p = problem
    log = []
    sol = await run(server, p["pid"], {"mode": mode, "targets": {"prop": 0.3},
                                       "penalties": {"boundary": 0.9}}, log=log)
    text = "\n".join(log)
    assert "Clustering round 0" in text and "Clustering round 1" in text, text[-2000:]
    chosen = {c for c, s in sol.items() if s == 1}
    assert p["locked_in"] in chosen and p["locked_out"] not in chosen
    if mode == "species":
        assert len(chosen) >= math.ceil(0.4 * len(sol))  # the 40% feature target still holds


async def test_area_mode_uses_the_budget_when_every_cell_is_expensive(server, problem):
    """The cost penalty is scaled to the grid. Unscaled (0.1 x cost 10 per
    cell) it outweighed a cell's whole contribution to the targets, so area
    mode selected nothing but the locked-in cell."""
    p = problem
    await server.pg.execute(
        "UPDATE bioprotect.cost_profile_values v SET cost = 10 FROM bioprotect.projects pr "
        "WHERE pr.id = %s AND v.cost_profile_id = pr.active_cost_profile_id", [p["pid"]])
    sol = await run(server, p["pid"], {"mode": "area", "targets": {"prop": 0.3}})
    chosen = [c for c, s in sol.items() if s == 1]
    # 8 cells here: the budget holds 2; the unscaled penalty picked only the locked-in one
    assert len(chosen) >= math.floor(0.3 * len(sol)), (len(chosen), len(sol))


async def test_area_mode_cohon_calibration_sets_the_cost_penalty(server, problem):
    p = problem
    log = []
    sol = await run(server, p["pid"], {"mode": "area", "targets": {"prop": 0.3},
                                       "penalties": {"cohon": True}}, log=log)
    text = "\n".join(log)
    assert "Cost penalty: Cohon" in text, text[-2000:]
    chosen = {c for c, s in sol.items() if s == 1}
    assert p["locked_in"] in chosen and p["locked_out"] not in chosen
