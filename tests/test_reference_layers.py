"""User reference layers (migration 0020): upload over the WebSocket, tiles,
delete rules, and all-or-nothing failure."""

import json
import os
import shutil
import tempfile
import uuid
import zipfile

import geopandas as gpd
import pytest
from shapely.geometry import LineString, Point, box

from conftest import api, import_folder, rows, ws_messages
import handlers.reference_layers_handler as ref_handler


def ref_zip(geoms, prj=True, **columns):
    """A zipped shapefile in the imports folder. Returns the zip's file name."""
    name = f"pytest_ref_{uuid.uuid4().hex[:8]}"
    shp_dir = tempfile.mkdtemp()
    gpd.GeoDataFrame(columns or {"label": [f"s{i}" for i in range(len(geoms))]},
                     geometry=geoms, crs=4326).to_file(os.path.join(shp_dir, f"{name}.shp"))
    if not prj:
        os.remove(os.path.join(shp_dir, f"{name}.prj"))
    with zipfile.ZipFile(os.path.join(import_folder(), f"{name}.zip"), "w") as z:
        for f in os.listdir(shp_dir):
            z.write(os.path.join(shp_dir, f), f)
    shutil.rmtree(shp_dir)
    return f"{name}.zip"


async def upload(server, zip_name, name=None, user="pytest_admin"):
    params = {"zipfile": zip_name}
    if name:
        params["name"] = name
    messages, _ = await ws_messages(server, "/server/importReferenceLayer", params,
                                    headers=server.headers_for(user))
    return messages


@pytest.fixture
async def cleanup(server):
    yield
    await server.pg.execute("DELETE FROM bioprotect.reference_layers WHERE name LIKE 'pytest%%'")


def leftover_files(zip_name):
    root = zip_name[:-4]
    return [f for f in os.listdir(import_folder()) if f.startswith(root)]


async def test_polygon_upload_tiles_and_listing(server, cleanup, monkeypatch):
    monkeypatch.setattr(ref_handler, "BATCH_POINTS", 10)  # force several batches
    circle = Point(-8.3, 51.6).buffer(0.3, 200)  # ~800 vertices: subdivided
    z = ref_zip([circle, box(-9, 51, -8.9, 51.1)], zone=["Cable corridor", "Wind lease"], code=[7, 9])
    messages = await upload(server, z, name="pytest wind zones")
    done = messages[-1]
    assert "error" not in done, messages
    assert done["status"] == "Finished"
    assert any("Preparing shapes (2/" in (m.get("info") or "") for m in messages)
    assert leftover_files(z) == []  # zip and unzipped parts deleted

    lid = done["id"]
    shapes = await rows(server.pg, "SELECT part, count(*) AS n, count(attributes) AS a "
                                   "FROM bioprotect.reference_layer_shapes WHERE layer_id = %s "
                                   "GROUP BY part ORDER BY part", [lid])
    by_part = {r["part"]: r for r in shapes}
    assert set(by_part) == {"fill", "outline"}
    assert by_part["fill"]["n"] > 2  # the circle was cut into pieces
    assert by_part["fill"]["a"] == by_part["fill"]["n"] and by_part["outline"]["a"] == 0
    attrs = (await rows(server.pg, "SELECT attributes FROM bioprotect.reference_layer_shapes "
                                   "WHERE layer_id = %s AND part = 'fill' LIMIT 1", [lid]))[0]["attributes"]
    assert set(attrs) == {"zone", "code"}

    # z7 tile over the circle carries data; a bad layer param gets nothing
    tile = (await rows(server.pg, "SELECT bioprotect.reference_tiles(7, 60, 42, %s) AS t",
                       [json.dumps({"layer": lid})]))[0]["t"]
    assert tile and len(tile) > 0
    assert (await rows(server.pg, "SELECT bioprotect.reference_tiles(7, 60, 42, %s) AS t",
                       ['{"layer": "x; drop"}']))[0]["t"] is None

    _, body = await api(server, "GET", "/server/reference-layers", user="pytest_user")
    listed = {l["name"]: l for l in body["layers"]}  # every user sees every layer
    assert listed["pytest wind zones"]["geom_type"] == "polygon"
    assert listed["pytest wind zones"]["colour"].startswith("#")


@pytest.mark.parametrize("geoms,part", [
    ([LineString([(-9, 51), (-8, 52)]), LineString([(-9, 52), (-8, 51)])], "line"),
    ([Point(-9, 51), Point(-8.5, 51.5)], "point"),
])
async def test_lines_and_points(server, cleanup, geoms, part):
    done = (await upload(server, ref_zip(geoms), name=f"pytest {part}s"))[-1]
    assert "error" not in done, done
    got = await rows(server.pg, "SELECT DISTINCT part FROM bioprotect.reference_layer_shapes "
                                "WHERE layer_id = %s", [done["id"]])
    assert [r["part"] for r in got] == [part]


async def test_missing_prj_and_name_clash_are_refused_and_leave_nothing(server, cleanup):
    z = ref_zip([box(-9, 51, -8.9, 51.1)], prj=False)
    done = (await upload(server, z, name="pytest no prj"))[-1]
    assert ".prj" in done.get("error", ""), done
    assert leftover_files(z) == []

    assert "error" not in (await upload(server, ref_zip([box(-9, 51, -8.9, 51.1)]), name="pytest Dup"))[-1]
    z = ref_zip([box(-9, 51, -8.9, 51.1)])
    done = (await upload(server, z, name="PYTEST dup"))[-1]  # case-insensitive
    assert "already exists" in done.get("error", ""), done
    assert leftover_files(z) == []
    assert len(await rows(server.pg, "SELECT 1 FROM bioprotect.reference_layers "
                                     "WHERE lower(name) IN ('pytest no prj', 'pytest dup')")) == 1


async def test_failed_upload_removes_the_layer(server, cleanup, monkeypatch):
    async def boom(*a, **k):
        raise ref_handler.ServicesError("disk on fire")
    real = server.pg.execute

    async def execute(q, *a, **k):
        if "reference_layer_add_shapes" in str(q):
            await boom()
        return await real(q, *a, **k)
    monkeypatch.setattr(server.pg, "execute", execute)
    done = (await upload(server, ref_zip([box(-9, 51, -8.9, 51.1)]), name="pytest broken"))[-1]
    monkeypatch.setattr(server.pg, "execute", real)
    assert done.get("error") == "disk on fire"
    assert await rows(server.pg, "SELECT 1 FROM bioprotect.reference_layers WHERE name = 'pytest broken'") == []


async def test_only_uploader_or_admin_can_delete(server, cleanup):
    done = (await upload(server, ref_zip([box(-9, 51, -8.9, 51.1)]), name="pytest mine",
                         user="pytest_admin"))[-1]
    lid = done["id"]
    _, body = await api(server, "POST", "/server/reference-layers", body={"action": "delete", "id": lid},
                        user="pytest_user")
    assert "error" in body
    done = (await upload(server, ref_zip([box(-9, 51, -8.9, 51.1)]), name="pytest theirs",
                         user="pytest_user"))[-1]
    _, body = await api(server, "POST", "/server/reference-layers",
                        body={"action": "delete", "id": done["id"]}, user="pytest_user")
    assert "error" not in body, body
    _, body = await api(server, "POST", "/server/reference-layers", body={"action": "delete", "id": lid})
    assert "error" not in body, body  # Admin
    assert await rows(server.pg, "SELECT 1 FROM bioprotect.reference_layer_shapes "
                                 "WHERE layer_id IN (%s, %s)", [lid, done["id"]]) == []
