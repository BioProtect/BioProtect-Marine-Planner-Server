"""
Shared raster utilities for activity and cost-profile ingest pipelines.

Provides:
  * reproject_to_wgs84(input_path, output_path)
      Reproject any raster to EPSG:4326 if not already in it.
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

import rasterio
from rasterio.warp import (
    Resampling,
    calculate_default_transform,
    reproject,
)


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
# exactextract names we accept from the frontend. Anything else -> weighted_mean
SUPPORTED_STATS = {
    "weighted_mean",   # area-weighted mean (default)
    "mean",            # simple mean of pixel values that touch the hex
    "sum",
    "min",
    "max",
    "count",
    "median",
}


async def extract_raster_to_hexes(
    raster_path: str,
    project_id: int,
    band: int,
    stat: str,
    pg,
) -> list[dict]:
    """Run exactextract zonal stats for every project hex.

    Fetches the hex polygons for the project from PostGIS, then uses
    exactextract to compute the chosen statistic for every hex against
    the chosen raster band. Hexes outside the raster's coverage return
    NaN, which the caller (normalise_costs) replaces with a fill value.

    Args:
        raster_path: Path to a WGS84 raster on disk.
        project_id: bioprotect.projects.id.
        band: 1-based band index to sample.
        stat: One of SUPPORTED_STATS.
        pg: Database access object exposing async ``execute``.

    Returns:
        List of {project_pu_id, h3_index, value} dicts. ``value`` may be
        NaN where the raster does not cover the hex.
    """
    # Local import: exactextract is heavy and only needed in this path.
    from exactextract import exact_extract  # type: ignore

    if stat not in SUPPORTED_STATS:
        stat = "weighted_mean"

    # Pull hex polygons as GeoJSON features. ST_AsGeoJSON gives a string;
    # we wrap each row into a Feature dict that exactextract accepts.
    rows = await pg.execute(
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

    if not rows:
        return []

    import json
    features = []
    for r in rows:
        features.append({
            "type": "Feature",
            "properties": {
                "project_pu_id": r["project_pu_id"],
                "h3_index": r["h3_index"],
            },
            "geometry": json.loads(r["geom_json"]),
        })

    # exactextract accepts a path string for the raster + list of features.
    # We restrict to the chosen band by passing include_cols + band index
    # via the dataset-opening hook below.
    with rasterio.open(raster_path) as src:
        if band < 1 or band > src.count:
            raise ValueError(
                f"Band {band} out of range (raster has {src.count} bands)."
            )

        # exactextract takes a rasterio dataset and a 1-based band index
        # through the `rast` kwarg. We pass a rasterio.Band to scope to
        # the requested band.
        rast_band = rasterio.band(src, band)

        results = exact_extract(
            rast=rast_band,
            vec=features,
            ops=[stat],
            include_cols=["project_pu_id", "h3_index"],
            output="pandas",
        )

    # exactextract pandas output: one row per feature, with columns:
    # project_pu_id, h3_index, <stat>
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
        out.append({
            "project_pu_id": int(row["project_pu_id"]),
            "h3_index": str(row["h3_index"]),
            "value": (
                float(row[stat_col])
                if row[stat_col] is not None and not _is_nan(row[stat_col])
                else float("nan")
            ),
        })
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
    clamp_negative: bool = True,
    fill_strategy: str = "median",
) -> tuple[dict[int, float], dict]:
    """Apply log(X+1) -> rescale-to-[floor, 1] and fill coverage gaps.

    Following Halpern et al. 2015 (Nat. Comms.), each pixel value is
    log(X+1) transformed then rescaled to [0, 1] by dividing by the
    global maximum transformed value. Here we *additionally* enforce a
    minimum floor > 0 so no hex ends up with cost = 0 (which would make
    it always-preferred by Prioritizr).

    Args:
        values: Iterable of {project_pu_id, value} dicts.
        floor: Minimum allowed cost (default 1e-3). Must be > 0.
        normalise: If False, skip log+rescale and only enforce floor.
        clamp_negative: If True, negative values are bumped to 0 before
            the log transform.
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

    raw_vals = []
    for v in covered:
        x = float(v["value"])
        if clamp_negative and x < 0:
            x = 0.0
        raw_vals.append(x)

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

    out: dict[int, float] = {}
    for v, cost in zip(covered, normalised):
        out[v["project_pu_id"]] = cost

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
