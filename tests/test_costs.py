"""Cost profiles from the handlers: raster upload, the raster library, and
the cumulative impact WebSocket. Grids and projects come from conftest;
each test deletes the rasters, activities and features it made.
"""

import os
import uuid

import numpy as np
import pytest
import rasterio
from rasterio.transform import from_bounds

from conftest import api, create_project, import_folder, rows, ws_messages

FLOOR = 0.001
SPLIT = -9.55  # raster value / feature amount changes here; AREA spans -9.6..-9.5


def write_raster(path, pixels=30):
    """pixels x pixels over -9.7..-9.4, 52.9..53.2 (wider than AREA): 0 west
    of SPLIT, 99 east of it. log1p normalisation then gives exactly FLOOR
    west and 1.0 east."""
    lons = -9.7 + (np.arange(pixels) + 0.5) * 0.3 / pixels
    data = np.tile(np.where(lons < SPLIT, 0, 99), (pixels, 1)).astype("float32")
    with rasterio.open(path, "w", driver="GTiff", width=pixels, height=pixels, count=1,
                       dtype="float32", crs="EPSG:4326",
                       transform=from_bounds(-9.7, 52.9, -9.4, 53.2, pixels, pixels)) as dst:
        dst.write(data, 1)


def side(cell):
    """'west' / 'east' when the whole hex is clear of SPLIT, else None."""
    import h3
    lons = [lon for _, lon in h3.cell_to_boundary(cell)]
    return "west" if max(lons) < SPLIT else "east" if min(lons) > SPLIT else None


async def profile_costs(pg, cp):
    got = await rows(pg, "SELECT pp.h3_index, cpv.cost FROM bioprotect.cost_profile_values cpv "
                         "JOIN bioprotect.project_pus pp ON pp.id = cpv.project_pu_id "
                         "WHERE cpv.cost_profile_id = %s", [cp])
    return {r["h3_index"]: float(r["cost"]) for r in got}


async def upload(server, pid, filename, user="pytest_admin"):
    """Profile names are unique per project, so each upload gets its own."""
    messages, _ = await ws_messages(server, "/server/uploadRasterCost", {
        "project_id": pid, "filename": filename,
        "profile_name": f"pytest raster {uuid.uuid4().hex[:6]}"},
        headers=server.headers_for(user))
    return messages


@pytest.fixture
async def rasters(server):
    """Collects raster ids to delete afterwards."""
    ids = []
    yield ids
    for rid in ids:
        await server.pg.execute("DELETE FROM bioprotect.cost_rasters WHERE id = %s", [rid])


# --- raster upload ----------------------------------------------------------

async def test_raster_upload_builds_normalised_active_profile(server, grid, rasters):
    alias, _, cells = grid
    pid = await create_project(server, alias)
    name = f"pytest_{uuid.uuid4().hex[:6]}.tif"
    write_raster(os.path.join(import_folder(), name))

    done = (await upload(server, pid, name))[-1]
    assert "error" not in done, done
    rasters.append(done["raster_id"])

    assert done["covered"] == done["total"] == len(cells)
    costs = await profile_costs(server.pg, done["cost_profile_id"])
    assert set(costs) == set(cells)
    for cell, cost in costs.items():
        expected = {"west": FLOOR, "east": 1.0}.get(side(cell))
        if expected is None:
            assert FLOOR <= cost <= 1.0
        else:
            assert cost == pytest.approx(expected), cell
    assert {side(c) for c in cells} >= {"west", "east"}  # both cases were exercised
    active = await rows(server.pg, "SELECT active_cost_profile_id AS id FROM bioprotect.projects "
                                   "WHERE id = %s", [pid])
    assert active[0]["id"] == done["cost_profile_id"]
    # the file is never kept
    assert not os.path.exists(os.path.join(import_folder(), name))


async def test_same_raster_again_reuses_the_cache(server, grid, rasters):
    pid = await create_project(server, grid[0])
    for _ in range(2):
        name = f"pytest_{uuid.uuid4().hex[:6]}.tif"  # same bytes, new name
        write_raster(os.path.join(import_folder(), name))
        messages = await upload(server, pid, name)
        assert "raster_id" in messages[-1], messages[-1]
        rasters.append(messages[-1]["raster_id"])
    assert rasters[0] == rasters[1]
    assert any("reusing cached values" in m.get("info", "") for m in messages)


async def test_raster_with_pixels_larger_than_the_grid(server, grid, rasters):
    pid = await create_project(server, grid[0])
    name = f"pytest_{uuid.uuid4().hex[:6]}.tif"
    write_raster(os.path.join(import_folder(), name), pixels=1)  # one 0.3° pixel
    done = (await upload(server, pid, name))[-1]
    rasters.extend([done["raster_id"]] if "raster_id" in done else [])
    assert "error" not in done, done


async def test_raster_upload_refuses_a_missing_file(server, grid):
    pid = await create_project(server, grid[0])
    done = (await upload(server, pid, "no_such.tif"))[-1]
    assert "Uploaded raster not found" in done["error"]


# --- raster library ---------------------------------------------------------

@pytest.fixture
async def cached_raster(server, grid, rasters):
    """A raster uploaded by pytest_admin through project 1. Yields (raster_id, alias)."""
    pid = await create_project(server, grid[0])
    name = f"pytest_{uuid.uuid4().hex[:6]}.tif"
    write_raster(os.path.join(import_folder(), name))
    done = (await upload(server, pid, name))[-1]
    rasters.append(done["raster_id"])
    yield done["raster_id"], grid[0]


async def library(server, user, **body):
    return (await api(server, "POST", "/server/costRasters", None, body, user=user))[1]


async def listed(server, user):
    _, body = await api(server, "GET", "/server/costRasters", {"action": "list"}, user=user)
    return {r["id"]: r for r in body["data"]}


async def test_reuse_builds_a_profile_for_another_project_without_upload(server, cached_raster):
    rid, alias = cached_raster
    p2 = await create_project(server, alias)
    body = await library(server, "pytest_admin", action="create_profile", raster_id=rid, project_id=p2)
    assert body["coverage_pct"] == 100.0
    assert len(await profile_costs(server.pg, body["cost_profile_id"])) == body["total"]


async def test_private_raster_is_invisible_and_undeletable_for_others(server, cached_raster):
    rid, _ = cached_raster
    assert rid not in await listed(server, "pytest_user")
    body = await library(server, "pytest_user", action="delete", raster_id=rid)
    assert "not allowed" in body["error"]


async def test_shared_raster_is_listed_but_still_only_owner_or_admin_deletes(server, cached_raster):
    rid, _ = cached_raster
    assert (await library(server, "pytest_admin", action="set_visibility",
                          raster_id=rid, visibility="shared"))["info"] == "Raster is now shared."
    theirs = await listed(server, "pytest_user")
    assert rid in theirs and theirs[rid]["can_manage"] is False
    assert "not allowed" in (await library(server, "pytest_user", action="delete", raster_id=rid))["error"]
    assert "not allowed" in (await library(server, "pytest_user", action="set_visibility",
                                           raster_id=rid, visibility="private"))["error"]


async def test_deleting_a_raster_keeps_profiles_built_from_it(server, cached_raster):
    rid, _ = cached_raster
    built = await rows(server.pg, "SELECT id FROM bioprotect.cost_profiles WHERE source_raster_id = %s", [rid])
    assert (await library(server, "pytest_admin", action="delete", raster_id=rid)) == \
        {"info": "Cost raster deleted."}
    after = await rows(server.pg, "SELECT source_raster_id FROM bioprotect.cost_profiles "
                                  "WHERE id = ANY(%s)", [[b["id"] for b in built]])
    assert len(after) == len(built) > 0 and all(r["source_raster_id"] is None for r in after)
    assert await rows(server.pg, "SELECT 1 FROM bioprotect.raster_hex_values WHERE raster_id = %s", [rid]) == []


async def test_shared_raster_cannot_be_applied_to_someone_elses_project(server, cached_raster):
    rid, alias = cached_raster
    await library(server, "pytest_admin", action="set_visibility", raster_id=rid, visibility="shared")
    p2 = await create_project(server, alias)  # pytest_admin's
    body = await library(server, "pytest_user", action="create_profile", raster_id=rid, project_id=p2)
    assert "error" in body


# --- cumulative impact WebSocket ---------------------------------------------

@pytest.fixture
async def activity(server):
    """A real PAD activity whose footprint covers all of AREA.
    Yields (activity_id, {pressure title: rppscore})."""
    pg = server.pg
    title = (await rows(pg, "SELECT activitytitle FROM bioprotect.pad WHERE rppscore > 0 "
                            "GROUP BY 1 ORDER BY count(*) LIMIT 1"))[0]["activitytitle"]
    pressures = {r["pressuretitle"]: float(r["rppscore"]) for r in await rows(
        pg, "SELECT pressuretitle, rppscore FROM bioprotect.pad "
            "WHERE activitytitle = %s AND rppscore > 0", [title])}
    table = f"pytest_act_{uuid.uuid4().hex[:6]}"
    await pg.execute(f"CREATE TABLE bioprotect.{table} AS "
                     "SELECT ST_MakeEnvelope(-9.7, 52.9, -9.4, 53.2, 4326) AS geometry")
    aid = (await rows(pg, "INSERT INTO bioprotect.metadata_activities (activity, activity_name) "
                          "VALUES (%s, %s) RETURNING id", [title, table]))[0]["id"]
    yield aid, pressures
    await pg.execute("DELETE FROM bioprotect.pressures WHERE activity_id = %s", [aid])
    await pg.execute("DELETE FROM bioprotect.metadata_activities WHERE id = %s", [aid])
    await pg.execute(f"DROP TABLE bioprotect.{table}")


async def run_ci(server, pid, aid):
    messages, _ = await ws_messages(server, "/server/runCumulativeImpact", {
        "project_id": pid, "activity_ids": str(aid), "profile_name": "pytest ci",
        "set_active": "true"})
    return messages[-1]


async def test_cumulative_impact_profile_from_activity_features_and_sensitivity(server, grid, activity):
    """Every hex is fully under the activity and the feature, with sensitivity 1
    for each of the activity's pressures, so impact = sum(rpp) x amount.
    Amount 1.0 west of SPLIT and 0.5 east gives costs 1.0 and the midpoint."""
    alias, _, cells = grid
    aid, pressures = activity
    pid = await create_project(server, alias)
    name = f"pytest_hab_{uuid.uuid4().hex[:6]}"
    fid = (await rows(server.pg, "INSERT INTO bioprotect.metadata_interest_features "
                                 "(feature_class_name, alias) VALUES (%s, %s) RETURNING unique_id",
                      [name, name]))[0]["unique_id"]
    try:
        await server.pg.execute("INSERT INTO bioprotect.project_features (project_id, feature_unique_id) "
                                "VALUES (%s, %s)", [pid, fid])
        for c in cells:
            await server.pg.execute("INSERT INTO bioprotect.pu_feature_amounts VALUES (%s, %s, %s, %s)",
                                    [pid, fid, c, 1.0 if side(c) == "west" else 0.5])
        for p in pressures:
            await server.pg.execute("INSERT INTO bioprotect.sensitivity_matrix "
                                    "(eunis_code, pressure, sensitivity_score) VALUES (%s, %s, 1)", [name, p])

        done = await run_ci(server, pid, aid)
        assert "error" not in done, done
        assert done["total_pressures"] == len(pressures)

        costs = await profile_costs(server.pg, done["cost_profile_id"])
        assert costs == {c: pytest.approx(1.0 if side(c) == "west" else FLOOR + (1 - FLOOR) * 0.5)
                         for c in cells}
        active = await rows(server.pg, "SELECT active_cost_profile_id AS id FROM bioprotect.projects "
                                       "WHERE id = %s", [pid])
        assert active[0]["id"] == done["cost_profile_id"]
    finally:
        await server.pg.execute("DELETE FROM bioprotect.sensitivity_matrix WHERE eunis_code = %s", [name])
        await server.pg.execute("DELETE FROM bioprotect.metadata_interest_features WHERE unique_id = %s", [fid])


async def test_cumulative_impact_refuses_a_project_without_preprocessed_features(server, grid, activity):
    pid = await create_project(server, grid[0])
    done = await run_ci(server, pid, activity[0])
    assert "No features have been preprocessed" in done["error"]
