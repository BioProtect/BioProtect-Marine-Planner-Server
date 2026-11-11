"""Cost raster library maths. No DB, no fixtures.

Covers the two bits that are easy to get silently wrong: the clip window
(a bad envelope means the extraction samples the wrong pixels, and you
only find out when the costs look plausible but are not) and the
NaN-for-missing-row contract between the cache and normalise_costs.
"""

import math
import os
import tempfile

import numpy as np
import rasterio
from rasterio.transform import from_origin

from services.raster_service import (
    bounds_wgs84,
    clip_and_reproject,
    normalise_costs,
    pick_sampling_method,
    pixel_area_km2,
    sample_raster_at_centroids,
)


def _write_raster(path, crs="EPSG:4326"):
    """10x10 raster over lon 0..10, lat 40..50, value = column index."""
    data = np.tile(np.arange(10, dtype="float32"), (10, 1))
    profile = {
        "driver": "GTiff", "height": 10, "width": 10, "count": 1,
        "dtype": "float32", "crs": crs,
        "transform": from_origin(0.0, 50.0, 1.0, 1.0),
    }
    with rasterio.open(path, "w", **profile) as dst:
        dst.write(data, 1)


def test_clip_keeps_the_right_pixels():
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "src.tif")
        out = os.path.join(d, "out.tif")
        _write_raster(src)

        assert bounds_wgs84(src) == (0.0, 40.0, 10.0, 50.0)

        # Clip to lon 3..6, lat 44..47.
        clip_and_reproject(src, out, (3.0, 44.0, 6.0, 47.0))
        with rasterio.open(out) as ds:
            b = ds.bounds
            assert (b.left, b.bottom, b.right, b.top) == (3.0, 44.0, 6.0, 47.0), b
            # value == column index, so the clipped block starts at 3.
            assert ds.read(1)[0].tolist() == [3.0, 4.0, 5.0], ds.read(1)[0]
        print("ok  clip_and_reproject keeps the requested window")

        # A clip box that misses the raster must fail loudly, not silently
        # return an empty extraction that looks like "no coverage".
        try:
            clip_and_reproject(src, out, (100.0, 10.0, 101.0, 11.0))
        except ValueError:
            print("ok  non-overlapping clip raises")
        else:
            raise AssertionError("expected ValueError for a disjoint clip box")


def test_sampling_method_follows_pixel_size():
    # 0.1 degree pixels at ~50N are ~80 km2. A res-7 hex is 5.2 km2, so it
    # sits well inside one pixel -> centroid. A res-5 hex is 253 km2 and
    # spans several -> exactextract.
    pix = 80.0
    assert pick_sampling_method(pix, 7) == "centroid"
    assert pick_sampling_method(pix, 8) == "centroid"
    assert pick_sampling_method(pix, 6) == "exact"
    assert pick_sampling_method(pix, 5) == "exact"
    # Unknown resolution must never silently take the approximate path.
    assert pick_sampling_method(pix, 99) == "exact"
    assert pick_sampling_method(0.0, 9) == "exact"
    print("ok  sampling method follows the pixel/hex ratio")


def test_centroid_sampling_reads_the_right_pixel():
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "src.tif")
        _write_raster(src)

        # value == column index, and the raster spans lon 0..10 at 1 deg.
        points = [
            {"h3_index": "a", "lon": 0.5, "lat": 49.5},   # col 0
            {"h3_index": "b", "lon": 4.5, "lat": 45.5},   # col 4
            {"h3_index": "c", "lon": 9.5, "lat": 40.5},   # col 9
            {"h3_index": "d", "lon": 40.0, "lat": 45.0},  # off the raster
        ]
        out = {r["h3_index"]: r["value"] for r in
               sample_raster_at_centroids(src, points, 1)}
        assert out["a"] == 0.0, out
        assert out["b"] == 4.0, out
        assert out["c"] == 9.0, out
        assert math.isnan(out["d"]), out
        print("ok  centroid sampling reads the pixel under each hex")

        # A pixel here is 1 degree square; at ~45N that is ~8700 km2.
        area = pixel_area_km2(src)
        assert 7000 < area < 10000, area
        print(f"ok  pixel area computed as {area:,.0f} km2")


def test_strip_sampling_matches_a_whole_band_read(monkeypatch):
    """Sampling reads the band strip by strip so a huge raster never lands in
    memory (prod OOM, 2026-11-10). Tiny strips here force every point through
    a different strip; the answers must be what a whole-band read gives."""
    import services.raster_service as rs
    with tempfile.TemporaryDirectory() as d:
        src = os.path.join(d, "src.tif")
        data = np.arange(100, dtype="float32").reshape(10, 10)  # unique per pixel
        with rasterio.open(src, "w", driver="GTiff", height=10, width=10, count=1,
                           dtype="float32", crs="EPSG:4326", nodata=55,
                           transform=from_origin(0.0, 50.0, 1.0, 1.0)) as dst:
            dst.write(data, 1)
        rng = np.random.default_rng(0)
        points = [{"h3_index": str(i), "lon": float(x), "lat": float(y)}
                  for i, (x, y) in enumerate(zip(rng.uniform(-1, 11, 300),
                                                 rng.uniform(39, 51, 300)))]
        monkeypatch.setattr(rs, "STRIP_PIXELS", 20)  # 2 rows per strip
        got = {r["h3_index"]: r["value"] for r in sample_raster_at_centroids(src, points, 1)}
        for p in points:
            col, row = math.floor(p["lon"]), math.floor(50 - p["lat"])
            want = (float(data[row, col]) if 0 <= row < 10 and 0 <= col < 10
                    and data[row, col] != 55 else float("nan"))
            assert (math.isnan(want) and math.isnan(got[p["h3_index"]])) \
                or got[p["h3_index"]] == want, (p, got[p["h3_index"]], want)


def test_reprojection_keeps_nodata_out_of_real_pixels():
    """The old in-memory reproject blended nodata (-9999) into its
    neighbours, so coverage edges came out as large negative costs."""
    with tempfile.TemporaryDirectory() as d:
        src, out = os.path.join(d, "merc.tif"), os.path.join(d, "out.tif")
        data = np.full((200, 200), 50.0, dtype="float32")
        data[:30, :] = -9999
        with rasterio.open(src, "w", driver="GTiff", height=200, width=200, count=1,
                           dtype="float32", crs="EPSG:3857", nodata=-9999,
                           transform=from_origin(-1_000_000, 7_000_000, 500, 500)) as dst:
            dst.write(data, 1)
        clip_and_reproject(src, out)
        with rasterio.open(out) as r:
            assert r.crs.to_epsg() == 4326
            v = r.read(1)
            real = v[v != r.nodata]
        assert real.size and real.min() >= 50.0 - 1e-3, real.min()


def test_missing_cache_rows_become_fills():
    # This is the cache -> profile contract: a hex with no cached row
    # arrives as NaN and must be filled, never treated as zero cost.
    values = [
        {"project_pu_id": 1, "value": 10.0},
        {"project_pu_id": 2, "value": 20.0},
        {"project_pu_id": 3, "value": float("nan")},   # not covered
    ]
    costs, info = normalise_costs(values, floor=0.001, fill_strategy="median")

    assert info["covered"] == 2 and info["total"] == 3, info
    assert set(costs) == {1, 2, 3}
    assert all(c > 0 for c in costs.values()), costs
    assert costs[3] == info["fill_value"]
    assert not any(math.isnan(c) for c in costs.values())
    print("ok  uncovered hexes are filled, never zero")

    # Every hex missing -> everything at the floor, still non-zero.
    costs, info = normalise_costs(
        [{"project_pu_id": 9, "value": float("nan")}], floor=0.05
    )
    assert costs == {9: 0.05}, costs
    assert info["coverage_pct"] == 0.0
    print("ok  zero coverage falls back to the floor")
