"""Full Prioritizr runs through the WebSocket: real R, real HiGHS/CBC.
Slow, so left out of the default run: `pytest -m slow`.

The problem is built so the optimum is known: every cell holds 1 unit of
one feature, west cells cost 1 and east cells cost 10, one east cell is
locked in and one west cell locked out.

The script picks the objective itself: min-set, or area (min shortfall,
budget 35% of the grid area) only when lock-outs leave a target unreachable.
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


async def set_locks(server, p, locked_in=(), locked_out=()):
    """Replace the project's PU statuses: 1 locked in, 2 locked out, else 0."""
    await server.pg.execute(
        "UPDATE bioprotect.cost_profile_values v "
        "   SET status = CASE WHEN pp.h3_index = ANY(%s) THEN 1 "
        "                     WHEN pp.h3_index = ANY(%s) THEN 2 ELSE 0 END "
        "  FROM bioprotect.project_pus pp, bioprotect.projects pr "
        " WHERE v.project_pu_id = pp.id AND pp.project_id = pr.id AND pr.id = %s "
        "   AND v.cost_profile_id = pr.active_cost_profile_id",
        [list(locked_in), list(locked_out), p["pid"]])


@pytest.fixture
async def area_problem(server, problem):
    """Only the locked-in cell and one cheap west cell stay unlocked: 2 of the
    cells hold too little of the feature for its 40% target, so the run must
    fall back to area mode. The budget (35% of ~8 near-equal cells) holds both."""
    p = problem
    p["available"] = {p["locked_in"], p["west"][1]}
    await set_locks(server, p, locked_in=[p["locked_in"]],
                    locked_out=[c for c in p["west"] + p["east"] if c not in p["available"]])
    return p


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

    log = []
    sol = await run(server, p["pid"], {"solver": {"gap": 0}}, log=log)

    assert "Mode: species" in "\n".join(log)
    assert set(sol) == set(p["west"]) | set(p["east"])  # one row per PU
    chosen = {c for c, s in sol.items() if s == 1}
    assert p["locked_in"] in chosen and p["locked_out"] not in chosen
    assert len(chosen) == need  # 1 unit per cell, so exactly the target
    assert chosen & set(p["east"]) == {p["locked_in"]}  # nothing expensive beyond the lock


async def test_a_100_percent_target_with_nothing_locked_out_stays_min_set(server, problem):
    """Exactly feasible: the old check (available >= required x 1.0001) sent
    this to area mode, whose 35% budget can't reach 100%."""
    p = problem
    await set_locks(server, p)
    await server.pg.execute("UPDATE bioprotect.project_features SET target_value = 100 "
                            "WHERE project_id = %s", [p["pid"]])
    log = []
    sol = await run(server, p["pid"], {}, log=log)
    assert "Mode: species" in "\n".join(log)
    assert all(s == 1 for s in sol.values())


async def test_an_unreachable_target_falls_back_to_area_mode(server, area_problem):
    p = area_problem
    log = []
    sol = await run(server, p["pid"], {"targets": {"prop": 0.3}}, log=log)
    assert "Mode: area" in "\n".join(log)
    # shortfall can only shrink by taking every reachable cell, and the budget holds them
    assert {c for c, s in sol.items() if s == 1} == p["available"]


async def test_area_budget_is_raised_to_the_locked_in_area(server, problem):
    """3 of ~8 near-equal cells locked in (> 35% of the area) and the rest
    locked out: unreachable target, and a 35% budget would be infeasible."""
    p = problem
    cells = p["west"] + p["east"]
    keep = cells[:3]
    await set_locks(server, p, locked_in=keep, locked_out=cells[3:])
    log = []
    sol = await run(server, p["pid"], {}, log=log)
    text = "\n".join(log)
    assert "Mode: area" in text and "Area budget raised" in text, text[-2000:]
    assert {c for c, s in sol.items() if s == 1} == set(keep)


async def test_min_set_clustering_rounds_keep_targets_and_locks(server, problem):
    p = problem
    log = []
    sol = await run(server, p["pid"], {"targets": {"prop": 0.3},
                                       "penalties": {"boundary": 0.9}}, log=log)
    text = "\n".join(log)
    assert "Mode: species" in text
    assert "Clustering round 0" in text and "Clustering round 1" in text, text[-2000:]
    chosen = {c for c, s in sol.items() if s == 1}
    assert p["locked_in"] in chosen and p["locked_out"] not in chosen
    assert len(chosen) >= math.ceil(0.4 * len(sol))  # the 40% feature target still holds


async def test_area_clustering_rounds_keep_locks(server, area_problem):
    p = area_problem
    log = []
    sol = await run(server, p["pid"], {"targets": {"prop": 0.3},
                                       "penalties": {"boundary": 0.9}}, log=log)
    text = "\n".join(log)
    assert "Mode: area" in text
    assert "Clustering round 0" in text and "Clustering round 1" in text, text[-2000:]
    chosen = {c for c, s in sol.items() if s == 1}
    assert p["locked_in"] in chosen and chosen <= p["available"]


async def test_area_mode_uses_the_budget_when_every_cell_is_expensive(server, area_problem):
    """The cost penalty is scaled to the grid. Unscaled (0.1 x cost 10 per
    cell) it outweighed a cell's whole contribution to the targets, so area
    mode selected nothing but the locked-in cell."""
    p = area_problem
    await server.pg.execute(
        "UPDATE bioprotect.cost_profile_values v SET cost = 10 FROM bioprotect.projects pr "
        "WHERE pr.id = %s AND v.cost_profile_id = pr.active_cost_profile_id", [p["pid"]])
    log = []
    sol = await run(server, p["pid"], {"targets": {"prop": 0.3}}, log=log)
    assert "Mode: area" in "\n".join(log)
    # the unscaled penalty picked only the locked-in cell
    assert {c for c, s in sol.items() if s == 1} == p["available"]


async def test_area_mode_cohon_calibration_sets_the_cost_penalty(server, area_problem):
    p = area_problem
    log = []
    sol = await run(server, p["pid"], {"targets": {"prop": 0.3},
                                       "penalties": {"cohon": True}}, log=log)
    text = "\n".join(log)
    assert "Mode: area" in text and "Cost penalty: Cohon" in text, text[-2000:]
    chosen = {c for c, s in sol.items() if s == 1}
    assert p["locked_in"] in chosen and chosen <= p["available"]
