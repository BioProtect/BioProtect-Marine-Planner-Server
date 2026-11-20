"""Features drawn on the map get land cut out (features?action=create-from-drawing)."""
import pytest

from tests.conftest import api


def _square(x0, y0, x1, y1):
    return {"type": "Feature", "properties": {}, "geometry": {
        "type": "Polygon", "coordinates": [[[x0, y0], [x1, y0], [x1, y1], [x0, y1], [x0, y0]]]}}


@pytest.fixture
async def land(server):
    # land covers the east half of the drawn square
    rows = await server.pg.execute(
        "INSERT INTO bioprotect.land (geom) VALUES (ST_MakeEnvelope(1, 0, 3, 2, 4326)) RETURNING id",
        return_format="Dict")
    yield
    await server.pg.execute("DELETE FROM bioprotect.land WHERE id = %s", [rows[0]["id"]])


async def _draw(server, name, *features):
    return await api(server, "POST", "/server/features", {"action": "create-from-drawing"},
                     body={"name": name, "description": "test",
                           "geojson": {"type": "FeatureCollection", "features": list(features)}})


async def test_drawn_feature_has_land_cut_out(server, land):
    code, body = await _draw(server, "pytest drawn seagrass", _square(0, 0, 2, 1))
    assert code == 200 and "error" not in body, body
    table = body["feature_class_name"]
    try:
        r = (await server.pg.execute(
            f'SELECT ST_Area(ST_Union(geometry)) AS a, ST_XMax(ST_Extent(geometry)) AS xmax '
            f'FROM bioprotect."{table}"', return_format="Dict"))[0]
        assert r["a"] == pytest.approx(1.0)  # 2x1 drawn, 1x1 on land
        assert r["xmax"] == pytest.approx(1.0)
    finally:
        await server.pg.execute(f'DROP TABLE IF EXISTS bioprotect."{table}"')
        await server.pg.execute(
            "DELETE FROM bioprotect.metadata_interest_features WHERE feature_class_name = %s", [table])


async def test_drawing_entirely_on_land_is_refused(server, land):
    code, body = await _draw(server, "pytest all land", _square(1.5, 0.5, 2.5, 1.5))
    assert "entirely on land" in body["error"]
    leftover = await server.pg.execute(
        "SELECT 1 FROM bioprotect.metadata_interest_features WHERE alias = 'pytest all land'")
    assert not leftover


async def test_drawing_over_open_sea_is_kept_whole(server, land):
    # no land under it: the empty fallback must carry SRID 4326 (mixed-SRID error 2026-11-20)
    code, body = await _draw(server, "pytest open sea", _square(-3, 0, -2, 1))
    assert "error" not in body, body
    table = body["feature_class_name"]
    try:
        r = (await server.pg.execute(
            f'SELECT ST_Area(ST_Union(geometry)) AS a FROM bioprotect."{table}"',
            return_format="Dict"))[0]
        assert r["a"] == pytest.approx(1.0)
    finally:
        await server.pg.execute(f'DROP TABLE IF EXISTS bioprotect."{table}"')
        await server.pg.execute(
            "DELETE FROM bioprotect.metadata_interest_features WHERE feature_class_name = %s", [table])
