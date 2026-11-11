"""Regression tests for the feature-import fixes of 2026-11-05.

- The split field arrives in its .dbf case (ET_Lvl3), but ogr2ogr lowercases
  columns in PostGIS, so the split used to fail with "column does not exist".
- 'A or B' values belong to both feature A and feature B.
- unzip_shapefile names the shapefile after its .shp, not the zip's first
  member, and counts .shp files instead of repeated extensions.
"""

import os
import shutil
import zipfile

import geopandas as gpd
import pytest
from shapely.geometry import box

from conftest import ws_messages
from services.file_service import unzip_shapefile
from services.service_error import ServicesError

CSO_SAMPLE = os.path.join("test-data", "cso_marine_sample.zip")


# --- unzip_shapefile -------------------------------------------------------

def _zip(folder, name, members):
    path = os.path.join(folder, name)
    with zipfile.ZipFile(path, "w") as z:
        for m in members:
            z.writestr(m, "x")
    return name


def test_unzip_names_shapefile_after_the_shp_not_the_first_member(tmp_path):
    name = _zip(tmp_path, "u.zip", ["map.png", "area.dbf", "area.shp", "area.shx"])
    assert unzip_shapefile(str(tmp_path), name) == "area"


def test_unzip_accepts_shp_xml_alongside_xml_metadata(tmp_path):
    # ArcGIS exports: area.shp.xml and area.xml are both '.xml'
    name = _zip(tmp_path, "u.zip",
                ["area.shp", "area.shx", "area.dbf", "area.shp.xml", "area.xml"])
    assert unzip_shapefile(str(tmp_path), name) == "area"


@pytest.mark.parametrize("members, message", [
    (["a.shp", "a.shx", "a.dbf", "b.shp", "b.shx", "b.dbf"], "multiple shapefiles"),
    (["map.png", "readme.txt"], "does not contain a .shp"),
])
def test_unzip_rejects_two_or_no_shapefiles(tmp_path, members, message):
    name = _zip(tmp_path, "u.zip", members)
    with pytest.raises(ServicesError, match=message):
        unzip_shapefile(str(tmp_path), name)


# --- split import through the real WebSocket handler -----------------------

def _import_folder():
    from classes.folder_path_config import get_folder_path_config
    return get_folder_path_config().IMPORT_FOLDER


async def _split_import(server, zip_path, splitfield):
    """Upload-and-unzip as the dialog does, then run the split import.
    Returns (messages, close_code, {feature name: polygon count})."""
    folder = _import_folder()
    shutil.copy(zip_path, folder)
    root = unzip_shapefile(folder, os.path.basename(zip_path))
    messages, code = await ws_messages(server, "/server/importFeatures",
                                       {"shapefile": f"{root}.shp",
                                        "splitfield": splitfield})
    created = {m["feature_class_name"]: m for m in messages
               if m.get("status") == "FeatureCreated"}
    counts = {}
    for table in created:
        meta = (await server.pg.execute(
            "SELECT unique_id, alias, _area, extent FROM bioprotect.metadata_interest_features "
            "WHERE feature_class_name = %s", [table], return_format="Array"))[0]
        # the frontend fetches the new feature by this id ([object Object] on prod 2026-11-10)
        assert created[table]["id"] == meta["unique_id"]
        assert meta["_area"] > 0 and meta["extent"]
        alias = meta["alias"]
        n = (await server.pg.execute(
            f'SELECT count(*) AS n FROM bioprotect."{table}"',
            return_format="Array"))[0]["n"]
        counts[alias] = n
    return messages, code, counts, list(created)


@pytest.fixture
async def cleanup(server):
    """Drop the feature tables and metadata rows a test created."""
    tables = []
    yield tables
    for t in tables:
        await server.pg.execute(f'DROP TABLE IF EXISTS bioprotect."{t}"')
        await server.pg.execute(
            "DELETE FROM bioprotect.metadata_interest_features "
            "WHERE feature_class_name = %s", [t])


def _errors(messages):
    return [m["error"] for m in messages if "error" in m]


async def test_split_on_mixed_case_field_with_or_values(server, cleanup, tmp_path):
    gdf = gpd.GeoDataFrame({
        "HabClass": ["Rock", "Sand", "Rock or Sand", ""],
        "geometry": [box(-9.0, 53.0, -8.9, 53.1), box(-8.9, 53.0, -8.8, 53.1),
                     box(-8.8, 53.0, -8.7, 53.1), box(-8.7, 53.0, -8.6, 53.1)],
    }, crs=4326)
    shp_dir = tmp_path / "shp"
    shp_dir.mkdir()
    gdf.to_file(shp_dir / "habitats.shp")
    zp = tmp_path / "habitats.zip"
    with zipfile.ZipFile(zp, "w") as z:
        for f in shp_dir.iterdir():
            z.write(f, f.name)

    messages, code, counts, tables = await _split_import(server, zp, "HabClass")
    cleanup.extend(tables)

    assert _errors(messages) == []
    assert code == 1000
    # the 'Rock or Sand' polygon is in both; the empty row is in neither
    assert counts == {"Rock": 2, "Sand": 2}
    assert any("Skipping 1 row(s)" in m.get("info", "") for m in messages)


async def test_split_real_cso_sample_by_level_2(server, cleanup):
    """12 real CSO polygons, 4 per ET_Lvl2 class. The class names contain '&'."""
    messages, code, counts, tables = await _split_import(server, CSO_SAMPLE, "ET_Lvl2")
    cleanup.extend(tables)

    assert _errors(messages) == []
    assert counts == {
        "Biogenic Reefs & Animal Forests (12.2)": 4,
        "Sublittoral Rocky Substrates (12.4)": 4,
        "Sublittoral Sediments (12.3)": 4,
    }


def _habitats_zip(tmp_path, classes):
    gdf = gpd.GeoDataFrame({"HabClass": classes,
                            "geometry": [box(-9 + i / 10, 53, -8.9 + i / 10, 53.1)
                                         for i in range(len(classes))]}, crs=4326)
    shp_dir = tmp_path / "shp"
    shp_dir.mkdir()
    gdf.to_file(shp_dir / "habitats.shp")
    zp = tmp_path / "habitats.zip"
    with zipfile.ZipFile(zp, "w") as z:
        for f in shp_dir.iterdir():
            z.write(f, f.name)
    return zp


async def _aliases(server, names):
    got = await server.pg.execute(
        "SELECT alias FROM bioprotect.metadata_interest_features WHERE alias = ANY(%s)",
        [names], return_format="Array")
    return sorted(r["alias"] for r in got)


async def test_split_refuses_up_front_when_a_name_is_taken(server, cleanup, tmp_path):
    await server.pg.execute("INSERT INTO bioprotect.metadata_interest_features "
                            "(feature_class_name, alias) VALUES ('pytest_taken', 'Sand')")
    cleanup.append("pytest_taken")
    messages, _, _, tables = await _split_import(
        server, _habitats_zip(tmp_path, ["Rock", "Sand"]), "HabClass")
    cleanup.extend(tables)
    assert tables == []
    assert any("'Sand'" in e for e in _errors(messages))
    assert await _aliases(server, ["Rock"]) == []


async def test_failed_split_rolls_back_features_already_created(server, cleanup, tmp_path, monkeypatch):
    """CSO upload on prod 2026-11-10: the 7th feature timed out and the first 6
    stayed behind. Now a failure removes everything that upload created."""
    real = server.pg.get_geometry_type
    calls = []

    async def fail_second(table):  # fail on the 2nd feature's table
        if table not in calls:
            calls.append(table)
        if len(calls) == 2:
            raise ServicesError("Database query timed out")
        return await real(table)

    monkeypatch.setattr(server.pg, "get_geometry_type", fail_second)
    zp = _habitats_zip(tmp_path, ["Rock", "Sand"])
    shutil.copy(zp, _import_folder())
    root = unzip_shapefile(_import_folder(), zp.name)
    messages, _ = await ws_messages(server, "/server/importFeatures",
                                    {"shapefile": f"{root}.shp", "splitfield": "HabClass"})
    cleanup.extend(calls)
    assert "Database query timed out" in _errors(messages)
    assert any(m.get("status") == "FeatureCreated" for m in messages)  # Rock got made...
    assert await _aliases(server, ["Rock", "Sand"]) == []              # ...and removed
    for t in calls:
        left = await server.pg.execute("SELECT to_regclass(%s) AS t",
                                       [f'bioprotect."{t}"'], return_format="Array")
        assert left[0]["t"] is None


# --- preprocessFeature (batched, shared feature_pieces) ---------------------

async def test_preprocess_batches_and_reuses_pieces_across_projects(server, grid):
    from conftest import AREA, create_project, rows
    alias, grid_id, cells = grid
    pg = server.pg
    table = "pytest_pre_" + os.urandom(3).hex()
    # west half of AREA
    await pg.execute(f"CREATE TABLE bioprotect.{table} (id serial PRIMARY KEY, geometry geometry)")
    await pg.execute(f"INSERT INTO bioprotect.{table} (geometry) "
                     "VALUES (ST_MakeEnvelope(-9.6, 53.0, -9.55, 53.1, 4326))")
    fid = (await rows(pg, "INSERT INTO bioprotect.metadata_interest_features "
                          "(feature_class_name, alias) VALUES (%s, %s) RETURNING unique_id",
                      [table, table]))[0]["unique_id"]
    try:
        infos = []
        for _ in range(2):
            pid = await create_project(server, alias)
            messages, _ = await ws_messages(server, "/server/preprocessFeature", {
                "project_id": pid, "feature_id": fid, "planning_grid_id": grid_id})
            assert not any("error" in m for m in messages), messages
            done = messages[-1]  # the frontend resolves on Finished, so the result must be on it
            assert done["status"] == "Finished" and done["pu_count"] > 0
            n = (await rows(pg, "SELECT count(*) AS n FROM bioprotect.pu_feature_amounts "
                                "WHERE project_id = %s AND feature_unique_id = %s", [pid, fid]))[0]["n"]
            assert n == done["pu_count"]
            infos.append(" ".join(m.get("info", "") for m in messages))
        assert "Preparing feature geometry" in infos[0]
        assert "Preparing feature geometry" not in infos[1]  # pieces reused
        assert "Intersecting with planning units" in infos[1]
    finally:
        await pg.execute("DELETE FROM bioprotect.metadata_interest_features WHERE unique_id = %s", [fid])
        await pg.execute(f"DROP TABLE bioprotect.{table}")
    # FK cascade cleared the pieces
    assert (await rows(pg, "SELECT count(*) AS n FROM bioprotect.feature_pieces "
                           "WHERE feature_unique_id = %s", [fid]))[0]["n"] == 0
