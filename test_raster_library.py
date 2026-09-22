"""Self-check for the cost raster library maths. No DB, no fixtures.

    python -W ignore test_raster_library.py

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


if __name__ == "__main__":
    test_clip_keeps_the_right_pixels()
    test_sampling_method_follows_pixel_size()
    test_centroid_sampling_reads_the_right_pixel()
    test_missing_cache_rows_become_fills()
    print("\nall raster library checks passed")
