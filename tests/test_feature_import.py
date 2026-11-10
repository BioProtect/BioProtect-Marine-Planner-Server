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
        alias = (await server.pg.execute(
            "SELECT alias FROM bioprotect.metadata_interest_features "
            "WHERE feature_class_name = %s", [table], return_format="Array"))[0]["alias"]
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
