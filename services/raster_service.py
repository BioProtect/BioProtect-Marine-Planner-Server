"""
Shared raster utilities for activity and cost-profile ingest pipelines.

Provides:
  * reproject_to_wgs84(input_path, output_path)
      Reproject any raster to EPSG:4326 if not already in it.
  * clip_and_reproject(input_path, output_path, bounds_4326)
      Window-read a raster down to a target footprint, then reproject.
  * bounds_wgs84(path)
      The raster's footprint as (left, bottom, right, top) in EPSG:4326.
  * fetch_project_hexes(pg, project_id) / fetch_hexes_in_bounds(pg, bounds)
      The two hex sets we extract against.
  * pick_sampling_method(pixel_area_km2, resolution)
      exactextract, or a centroid point-sample when a hex is far smaller
      than a pixel and the two agree anyway.
  * sample_raster_at_centroids(raster_path, points, band)
      The cheap path: one vectorised numpy lookup for the whole set.
  * get_raster_band_info(path)
      Inspect a raster for band count, dtypes, nodata, bounds and CRS.
  * extract_raster_to_hexes(raster_path, project_id, band, stat, pg)
      Run exactextract zonal stats per project hex.
  * normalise_costs(values, floor, fill_strategy)
      Apply Halpern-style log(X+1) -> rescale-to-[floor, 1], compute a
      non-zero fill value for hexes outside raster coverage.

Cells must never receive a cost of zero, since Prioritizr would always
favour them. The normalisation enforces a configurable floor > 0 and
coverage gaps are filled with a sensible non-zero value.
"""

import logging
import math
import os
import shutil
import statistics
from typing import Iterable

import numpy as np
import rasterio
from tornado.ioloop import IOLoop
from rasterio.errors import WindowError
from rasterio.transform import array_bounds
from rasterio.warp import (
    Resampling,
    calculate_default_transform,
    reproject,
    transform_bounds,
)
from rasterio.windows import Window, from_bounds as window_from_bounds


log = logging.getLogger(__name__)


# ----------------------------------------------------------------------------
# Reprojection
# ----------------------------------------------------------------------------
def reproject_to_wgs84(input_path: str, output_path: str) -> None:
    """Reproject a raster to EPSG:4326. Copies the file if already 4326.

    Args:
        input_path: Path to the source raster on disk.
        output_path: Destination path for the reprojected raster.
    """
    with rasterio.open(input_path) as src:
        if src.crs and src.crs.to_epsg() == 4326:
            if input_path != output_path:
                shutil.copy2(input_path, output_path)
            return

        transform, width, height = calculate_default_transform(
            src.crs, "EPSG:4326",
            src.width, src.height, *src.bounds,
        )

        kwargs = src.meta.copy()
        kwargs.update({
            "crs": "EPSG:4326",
            "transform": transform,
            "width": width,
            "height": height,
        })

        with rasterio.open(output_path, "w", **kwargs) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, i),
                    destination=rasterio.band(dst, i),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs="EPSG:4326",
                    resampling=Resampling.bilinear,
                )


def bounds_wgs84(path: str) -> tuple[float, float, float, float]:
    """The raster's footprint as (left, bottom, right, top) in EPSG:4326."""
    with rasterio.open(path) as src:
        if src.crs is None:
            # No CRS to convert from; assume the numbers are already lat/lon.
            b = src.bounds
            return (b.left, b.bottom, b.right, b.top)
        return tuple(
            transform_bounds(src.crs, "EPSG:4326", *src.bounds, densify_pts=21)
        )


def clip_and_reproject(input_path, output_path, bounds_4326=None) -> None:
    """Window-read a raster to a WGS84 footprint, then reproject to 4326.

    The clip is to the hexes we are about to extract against, not to any one
    project - the raster is discarded either way, so nothing reusable is lost,
    and we avoid reprojecting the parts of a continental raster that no
    planning grid covers.

    ponytail: the window is read into memory in one go. Fine for a footprint
    that covers a few planning grids; if a single extraction ever spans most
    of a continent at full resolution, switch to block-wise warping.

    Args:
        input_path: Source raster on disk, any CRS.
        output_path: Destination path for the WGS84 result.
        bounds_4326: (left, bottom, right, top) to clip to, or None for the
            whole raster.

    Raises:
        ValueError: If the clip box does not overlap the raster.
    """
    with rasterio.open(input_path) as src:
        if src.crs is None:
            raise ValueError(
                "Raster has no CRS; cannot reproject. Assign one and re-upload."
            )

        window = None
        if bounds_4326 is not None:
            src_box = transform_bounds(
                "EPSG:4326", src.crs, *bounds_4326, densify_pts=21
            )
            window = window_from_bounds(*src_box, transform=src.transform)
            try:
                window = window.intersection(
                    Window(0, 0, src.width, src.height)
                )
            except WindowError as exc:
                # rasterio raises rather than returning an empty window
                raise ValueError(
                    "The raster does not overlap any planning grid hexes."
                ) from exc
            window = window.round_offsets().round_lengths()
            if window.width < 1 or window.height < 1:
                raise ValueError(
                    "The raster does not overlap any planning grid hexes."
                )

        data = src.read(window=window)
        src_transform = (
            src.transform if window is None else src.window_transform(window)
        )
        height, width = data.shape[1], data.shape[2]

        profile = src.profile.copy()
        profile.update(transform=src_transform, width=width, height=height)

        if src.crs.to_epsg() == 4326:
            with rasterio.open(output_path, "w", **profile) as dst:
                dst.write(data)
            return

        dst_transform, dst_w, dst_h = calculate_default_transform(
            src.crs, "EPSG:4326", width, height,
            *array_bounds(height, width, src_transform),
        )
        profile.update({
            "crs": "EPSG:4326",
            "transform": dst_transform,
            "width": dst_w,
            "height": dst_h,
        })
        with rasterio.open(output_path, "w", **profile) as dst:
            for i in range(1, src.count + 1):
                reproject(
                    source=data[i - 1],
                    destination=rasterio.band(dst, i),
                    src_transform=src_transform,
                    src_crs=src.crs,
                    dst_transform=dst_transform,
                    dst_crs="EPSG:4326",
                    resampling=Resampling.bilinear,
                )


# ----------------------------------------------------------------------------
# Band / metadata inspection
# ----------------------------------------------------------------------------
def get_raster_band_info(path: str) -> dict:
    """Return basic metadata for a raster file.

    Args:
        path: Path to the raster on disk.

    Returns:
        Dict with band_count, dtypes (list), nodata (list), bounds (dict),
        and crs (EPSG code or WKT string).
    """
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Raster file not found: {path}")

    with rasterio.open(path) as src:
        bounds = src.bounds
        epsg = src.crs.to_epsg() if src.crs else None
        return {
            "band_count": src.count,
            "dtypes": [str(d) for d in src.dtypes],
            "nodata": [
                (None if src.nodatavals[i] is None else float(src.nodatavals[i]))
                for i in range(src.count)
            ],
            "bounds": {
                "left": bounds.left,
                "bottom": bounds.bottom,
                "right": bounds.right,
                "top": bounds.top,
            },
            "crs_epsg": epsg,
            "crs_wkt": src.crs.to_wkt() if (src.crs and epsg is None) else None,
            "width": src.width,
            "height": src.height,
        }


# ----------------------------------------------------------------------------
# Zonal stats via exactextract
# ----------------------------------------------------------------------------
# exactextract names we accept from the frontend. Anything else -> mean.
#
# Note on weighting: exactextract's `mean` is ALREADY area-weighted by the
# fractional pixel coverage of each polygon. There is no need (and indeed
# no way) to use `weighted_mean` here because that operator requires a
# second "weights" raster (e.g. population density). For our use case
# of mapping a single cost raster into hex cells, `mean` is the correct
# area-weighted aggregation.
SUPPORTED_STATS = {
    "mean",     # area-weighted by pixel coverage (default)
    "sum",
    "min",
    "max",
    "count",    # sum of pixel coverage fractions in the polygon
    "median",
    "stdev",
    "variety",  # count of distinct values
}


# ----------------------------------------------------------------------------
# Choosing how to sample: exactextract vs a centroid lookup
# ----------------------------------------------------------------------------
# Average area of an H3 cell, km^2, by resolution (H3 v4 reference values).
H3_AREA_KM2 = {
    0: 4357449.4, 1: 609788.4, 2: 86801.8, 3: 12393.4, 4: 1770.3,
    5: 252.9, 6: 36.129, 7: 5.161, 8: 0.7373, 9: 0.1053,
    10: 0.01504, 11: 0.002148, 12: 0.000307, 13: 0.0000439,
    14: 0.0000063, 15: 0.0000009,
}

# How many times smaller than a pixel a hex must be before we stop paying
# for exactextract's area weighting and just read the pixel under the hex
# centroid. Once a hex sits inside a single pixel the area-weighted mean IS
# that pixel's value, so the two agree exactly; the only hexes that differ
# are the ones straddling a pixel boundary, which get one pixel's value
# instead of a blend of two.
#
# ponytail: 9 (hex ~1/3 of a pixel across) trades a small edge-blending
# error for roughly two orders of magnitude of speed. Raise it toward 25+
# if a cost surface ever needs the blend, or pass sampling='exact' to force
# exactextract for one upload.
CENTROID_SAMPLE_AREA_RATIO = 9.0


def pixel_area_km2(path: str) -> float:
    """Approximate area of one pixel, km^2, at the raster's mid-latitude.

    The raster is in EPSG:4326 by the time we sample it, so a pixel is a
    box in degrees whose ground size depends on latitude.
    """
    with rasterio.open(path) as src:
        dx, dy = src.res
        mid_lat = (src.bounds.bottom + src.bounds.top) / 2.0
        km_per_deg_lon = 111.32 * math.cos(math.radians(mid_lat))
        km_per_deg_lat = 110.57
        return abs(dx * km_per_deg_lon) * abs(dy * km_per_deg_lat)


def pick_sampling_method(pix_area: float, resolution, ratio=None) -> str:
    """'centroid' when a hex is far smaller than a pixel, else 'exact'."""
    ratio = CENTROID_SAMPLE_AREA_RATIO if ratio is None else ratio
    hex_area = H3_AREA_KM2.get(resolution)
    if hex_area is None or not pix_area:
        return "exact"
    return "centroid" if pix_area >= ratio * hex_area else "exact"


def sample_raster_at_centroids(raster_path, points, band: int) -> list[dict]:
    """Read the pixel under each point. One vectorised lookup, no polygons.

    This is the whole saving: no GeoJSON parsing, no per-polygon coverage
    maths, and no 3-million-feature list in memory - just three arrays.

    ponytail: reads the band into memory in one go, which is fine for the
    clipped footprints we sample. Switch to windowed reads if a single
    extraction ever needs a band bigger than RAM.

    Args:
        raster_path: WGS84 raster on disk.
        points: rows of {h3_index, lon, lat}.
        band: 1-based band index.

    Returns:
        List of {h3_index, value}; value is NaN off-raster or on nodata.
    """
    if not points:
        return []

    lons = np.fromiter((float(p["lon"]) for p in points), dtype="float64",
                       count=len(points))
    lats = np.fromiter((float(p["lat"]) for p in points), dtype="float64",
                       count=len(points))

    with rasterio.open(raster_path) as src:
        if band < 1 or band > src.count:
            raise ValueError(
                f"Band {band} out of range (raster has {src.count} bands)."
            )
        arr = src.read(band).astype("float64")
        nodata = src.nodatavals[band - 1]
        inv = ~src.transform
        height, width = src.height, src.width

    # Affine inverse: world -> fractional pixel, then floor to an index.
    cols = np.floor(inv.a * lons + inv.b * lats + inv.c).astype("int64")
    rows = np.floor(inv.d * lons + inv.e * lats + inv.f).astype("int64")

    inside = (
        (rows >= 0) & (rows < height) & (cols >= 0) & (cols < width)
    )
    values = np.full(len(points), np.nan, dtype="float64")
    values[inside] = arr[rows[inside], cols[inside]]

    if nodata is not None:
        values[values == nodata] = np.nan

    return [
        {"h3_index": p["h3_index"], "value": float(v)}
        for p, v in zip(points, values)
    ]


async def fetch_project_hexes(pg, project_id: int) -> list[dict]:
    """Hex rows for one project: {project_pu_id, h3_index, geom_json}."""
    return await pg.execute(
        """
        SELECT pp.id              AS project_pu_id,
               pp.h3_index        AS h3_index,
               ST_AsGeoJSON(hc.geometry) AS geom_json
          FROM bioprotect.project_pus pp
          JOIN bioprotect.h3_cells hc
            ON hc.h3_index = pp.h3_index
         WHERE pp.project_id = %s
         ORDER BY pp.id;
        """,
        data=[project_id],
        return_format="Array",
    )


async def fetch_hexes_in_bounds(pg, bounds_4326, resolutions=None) -> list[dict]:
    """Hex polygons intersecting a WGS84 footprint, for exactextract.

    This is the extraction target for the raster library: while the file is
    on disk we sample it once against everything it could ever be asked
    about, then throw the file away. The set is bounded by the grids that
    exist in h3_cells, not by the raster's area.

    The same h3_index can appear in h3_cells under more than one
    project_area (overlapping grids), hence DISTINCT ON.

    Args:
        pg: Database access object exposing async ``execute``.
        bounds_4326: (left, bottom, right, top) in EPSG:4326.
        resolutions: Restrict to these H3 resolutions, or None for all.

    Returns:
        List of {h3_index, geom_json} dicts.
    """
    left, bottom, right, top = bounds_4326
    res_filter = "" if not resolutions else " AND hc.resolution = ANY(%s)"
    data = [left, bottom, right, top]
    if resolutions:
        data.append(list(resolutions))
    return await pg.execute(
        f"""
        SELECT DISTINCT ON (hc.h3_index)
               hc.h3_index               AS h3_index,
               ST_AsGeoJSON(hc.geometry) AS geom_json
          FROM bioprotect.h3_cells hc
         WHERE hc.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326){res_filter}
         ORDER BY hc.h3_index;
        """,
        data=data,
        return_format="Array",
    )


async def fetch_hex_points_in_bounds(pg, bounds_4326, resolutions) -> list[dict]:
    """Hex centroids only - no geometry JSON, for the cheap sampling path.

    Deliberately does not select ST_AsGeoJSON: for sub-pixel hexes the
    polygon is never needed, and not materialising millions of GeoJSON
    strings is most of why this path is fast.

    Returns:
        List of {h3_index, lon, lat} dicts.
    """
    left, bottom, right, top = bounds_4326
    return await pg.execute(
        """
        SELECT DISTINCT ON (hc.h3_index)
               hc.h3_index                    AS h3_index,
               ST_X(ST_Centroid(hc.geometry)) AS lon,
               ST_Y(ST_Centroid(hc.geometry)) AS lat
          FROM bioprotect.h3_cells hc
         WHERE hc.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
           AND hc.resolution = ANY(%s)
         ORDER BY hc.h3_index;
        """,
        data=[left, bottom, right, top, list(resolutions)],
        return_format="Array",
    )


async def bounds_of_hexes_in_bounds(pg, bounds_4326) -> tuple | None:
    """Envelope of every hex inside a footprint, computed in PostGIS.

    Used to clip the raster before reprojecting. Doing it in SQL means we
    never need the geometries in Python just to work out a bounding box.
    """
    left, bottom, right, top = bounds_4326
    row = await pg.execute(
        """
        SELECT ST_XMin(e) AS left, ST_YMin(e) AS bottom,
               ST_XMax(e) AS right, ST_YMax(e) AS top
          FROM (
            SELECT ST_Extent(hc.geometry) AS e
              FROM bioprotect.h3_cells hc
             WHERE hc.geometry && ST_MakeEnvelope(%s, %s, %s, %s, 4326)
          ) x;
        """,
        data=[left, bottom, right, top],
        return_format="Array",
    )
    if not row or row[0]["left"] is None:
        return None
    r = row[0]
    return (r["left"], r["bottom"], r["right"], r["top"])


async def extract_raster_to_hexes(
    raster_path: str,
    rows: list[dict],
    band: int,
    stat: str,
) -> list[dict]:
    """Run exactextract zonal stats for a set of hex polygons.

    Args:
        raster_path: Path to a WGS84 raster on disk.
        rows: Hex rows from fetch_project_hexes / fetch_hexes_in_bounds.
            Each needs ``h3_index`` and ``geom_json``; ``project_pu_id`` is
            carried through when present.
        band: 1-based band index to sample.
        stat: One of SUPPORTED_STATS.

    Returns:
        List of {project_pu_id, h3_index, value} dicts. ``value`` may be
        NaN where the raster does not cover the hex.
    """
    # Local import: exactextract is heavy and only needed in this path.
    from exactextract import exact_extract  # type: ignore

    if stat not in SUPPORTED_STATS:
        stat = "mean"

    if not rows:
        return []

    import json
    has_pu_id = "project_pu_id" in rows[0]
    id_cols = ["h3_index"] + (["project_pu_id"] if has_pu_id else [])

    features = []
    for r in rows:
        props = {"h3_index": r["h3_index"]}
        if has_pu_id:
            props["project_pu_id"] = r["project_pu_id"]
        features.append({
            "type": "Feature",
            "properties": props,
            "geometry": json.loads(r["geom_json"]),
        })

    # exactextract.prep_raster only accepts gdal.Dataset, rasterio
    # DatasetReader, xarray DataArray/Dataset, numpy ndarray, or a path.
    # rasterio.band() is NOT supported, so for multi-band rasters we
    # materialise the chosen band into an in-memory single-band dataset
    # and pass that. Single-band rasters can be passed directly.
    from rasterio.io import MemoryFile  # type: ignore

    # ponytail: exact_extract is a blocking C call - run it off the IOLoop
    # so the websocket ping keeps firing on big grids.
    def _extract():
        with rasterio.open(raster_path) as src:
            if band < 1 or band > src.count:
                raise ValueError(
                    f"Band {band} out of range (raster has {src.count} bands)."
                )

            if src.count == 1:
                results = exact_extract(
                    rast=src,
                    vec=features,
                    ops=[stat],
                    include_cols=id_cols,
                    output="pandas",
                )
            else:
                band_data = src.read(band)
                profile = src.profile.copy()
                profile.update(count=1, dtype=band_data.dtype)
                with MemoryFile() as memfile:
                    with memfile.open(**profile) as mem_dst:
                        mem_dst.write(band_data, 1)
                    with memfile.open() as mem_src:
                        results = exact_extract(
                            rast=mem_src,
                            vec=features,
                            ops=[stat],
                            include_cols=id_cols,
                            output="pandas",
                        )
        return results

    results = await IOLoop.current().run_in_executor(None, _extract)

    # exactextract pandas output: one row per feature, with columns:
    # h3_index, [project_pu_id], <stat>
    out = []
    stat_col = stat
    if stat_col not in results.columns:
        # exactextract sometimes prefixes with band name. Find it.
        candidates = [c for c in results.columns if c.endswith(stat)]
        if not candidates:
            raise RuntimeError(
                f"exactextract did not return a '{stat}' column. "
                f"Got columns: {list(results.columns)}"
            )
        stat_col = candidates[0]

    for _, row in results.iterrows():
        rec = {
            "h3_index": str(row["h3_index"]),
            "value": (
                float(row[stat_col])
                if row[stat_col] is not None and not _is_nan(row[stat_col])
                else float("nan")
            ),
        }
        if has_pu_id:
            rec["project_pu_id"] = int(row["project_pu_id"])
        out.append(rec)
    return out


def _is_nan(x) -> bool:
    try:
        return math.isnan(x)
    except (TypeError, ValueError):
        return False


# ----------------------------------------------------------------------------
# Halpern-style normalisation with non-zero floor
# ----------------------------------------------------------------------------
def normalise_costs(
    values: Iterable[dict],
    floor: float = 1e-3,
    normalise: bool = True,
    fill_strategy: str = "median",
) -> tuple[dict[int, float], dict]:
    """Apply log(X+1) -> rescale-to-[floor, 1] and fill coverage gaps.

    Following Halpern et al. 2015 (Nat. Comms.), each pixel value is
    log(X+1) transformed then rescaled to [0, 1] by dividing by the
    global maximum transformed value. Here we *additionally* enforce a
    minimum floor > 0 so no hex ends up with cost = 0 (which would make
    it always-preferred by Prioritizr).

    Negative input values are ALWAYS forced to floor in the output.
    Negatives are nonsensical for a cost layer (and log(X+1) is undefined
    for X < -1), so they cannot be passed through. This is not optional.

    Args:
        values: Iterable of {project_pu_id, value} dicts.
        floor: Minimum allowed cost (default 1e-3). Must be > 0.
        normalise: If False, skip log+rescale and only enforce floor.
        fill_strategy: How to fill hexes the raster does not cover.
            ``"median"`` (default) -> median of normalised costs;
            ``"floor"``  -> the floor value;
            ``"max"``    -> 1.0 (uncovered = most expensive);
            otherwise interpreted as a literal float.

    Returns:
        Tuple of:
          * dict {project_pu_id: cost} for every input hex (covered AND
            uncovered).
          * info dict with coverage statistics and chosen fill value.
    """
    if floor <= 0:
        raise ValueError("floor must be > 0; cells can never have zero cost.")
    if floor >= 1:
        raise ValueError("floor must be < 1.")

    values = list(values)
    total = len(values)

    # Split covered (finite) vs uncovered (NaN) hexes
    covered = [v for v in values if not _is_nan(v["value"])]
    uncovered = [v for v in values if _is_nan(v["value"])]

    if not covered:
        # No hex got a value. Fill everything with the floor.
        return (
            {v["project_pu_id"]: floor for v in values},
            {
                "covered": 0,
                "total": total,
                "coverage_pct": 0.0,
                "fill_value": floor,
                "max_raw": None,
            },
        )

    # Track which inputs were negative so we can map them directly to
    # floor in the final output (independent of the rescale curve).
    # Clamping negatives to floor is mandatory: log(X+1) is undefined
    # for X < -1, and negative costs are nonsensical for Prioritizr.
    raw_vals = []
    was_negative = []
    for v in covered:
        x = float(v["value"])
        neg = x < 0
        if neg:
            # Working value of 0 so log(X+1) stays defined; the final
            # output is then forced to floor below.
            x = 0.0
        raw_vals.append(x)
        was_negative.append(neg)

    if normalise:
        log_vals = [math.log(x + 1.0) for x in raw_vals]
        max_log = max(log_vals) if log_vals else 1.0
        if max_log <= 0:
            # All zero raw values -> everything becomes floor
            normalised = [floor for _ in log_vals]
        else:
            normalised = [
                _scale_into_floor_unit(lv / max_log, floor) for lv in log_vals
            ]
    else:
        # Skip log+rescale but still clamp into [floor, 1] so the cost
        # vector remains valid for Prioritizr.
        max_raw = max(raw_vals) if raw_vals else 1.0
        if max_raw <= 0:
            normalised = [floor for _ in raw_vals]
        else:
            normalised = [
                _scale_into_floor_unit(x / max_raw, floor) for x in raw_vals
            ]

    # Force any clamped-negative input straight to floor, and as a final
    # safety net ensure every emitted cost is strictly >= floor. No hex
    # should ever leave this function with cost < floor, because zero or
    # near-zero costs would always be favoured by Prioritizr.
    out: dict[int, float] = {}
    for v, cost, neg in zip(covered, normalised, was_negative):
        final = floor if neg else cost
        if final < floor:
            final = floor
        elif final > 1.0:
            final = 1.0
        out[v["project_pu_id"]] = final

    # Choose a fill value
    fill_value = _choose_fill_value(normalised, fill_strategy, floor)
    for v in uncovered:
        out[v["project_pu_id"]] = fill_value

    info = {
        "covered": len(covered),
        "total": total,
        "coverage_pct": (len(covered) / total * 100.0) if total else 0.0,
        "fill_value": fill_value,
        "max_raw": max(raw_vals) if raw_vals else None,
    }
    return out, info


def _scale_into_floor_unit(unit_value: float, floor: float) -> float:
    """Map a value in [0, 1] into [floor, 1].

    Linear remap so 0 -> floor and 1 -> 1. Keeps the spread of the data
    while guaranteeing the lower bound is strictly positive.
    """
    if unit_value <= 0:
        return floor
    if unit_value >= 1:
        return 1.0
    return floor + (1.0 - floor) * unit_value


def _choose_fill_value(
    normalised: list[float],
    strategy: str,
    floor: float,
) -> float:
    """Pick a fill cost for hexes outside raster coverage.

    Must always return a value >= floor.
    """
    if not normalised:
        return floor

    if strategy == "median":
        v = statistics.median(normalised)
    elif strategy == "floor":
        v = floor
    elif strategy == "max":
        v = 1.0
    else:
        try:
            v = float(strategy)
        except (TypeError, ValueError):
            log.warning(
                "Unknown fill_strategy %r; defaulting to median.", strategy
            )
            v = statistics.median(normalised)

    return max(floor, min(1.0, v))
