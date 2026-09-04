"""
Build a downloadable GIS export of a project's planning-unit grid with
per-run Prioritizr selections attached.

The export contains:
  - pus.shp / pus.shx / pus.dbf / pus.prj   (shapefile mode)
    OR pus.gpkg                              (geopackage mode)
  - runs.csv     — maps `run_N` columns back to {run_id, label, created_at}
  - features.csv — per-feature target / achieved / runs-met summary

Shapefile field names are limited to 10 chars, so per-run columns are
emitted as `run_1`, `run_2`, … with the runs.csv sidecar as the legend.
"""

import logging
import os
import shutil
import uuid

import pandas as pd

from services.file_service import zip_folder
from services.run_command_service import run_command
from services.service_error import ServicesError


ALLOWED_FORMATS = ("shp", "gpkg")


def _validate_ids(project_id, run_ids):
    if not isinstance(project_id, int):
        raise ServicesError("project_id must be an integer")
    if not run_ids or not all(isinstance(r, int) for r in run_ids):
        raise ServicesError("run_ids must be a non-empty list of integers")


async def _resolve_active_cost_profile_id(pg, project_id):
    rows = await pg.execute(
        "SELECT active_cost_profile_id FROM bioprotect.projects WHERE id = %s",
        data=[project_id],
        return_format="Array",
    )
    if not rows:
        raise ServicesError(f"Project {project_id} not found")
    # may be None — projects without an active cost profile.
    return rows[0]["active_cost_profile_id"]


def _build_export_sql(table_name, project_id, run_ids, cost_profile_id):
    """Builds the CREATE UNLOGGED TABLE statement for the export."""
    n = len(run_ids)
    run_cols = ",\n      ".join(
        f"COALESCE(r{i + 1}.solution, 0)::int AS run_{i + 1}"
        for i in range(n)
    )
    sum_expr = " + ".join(f"COALESCE(r{i + 1}.solution, 0)" for i in range(n))
    run_joins = "\n    ".join(
        f"LEFT JOIN bioprotect.prioritizr_run_results r{i + 1} "
        f"ON r{i + 1}.h3_index = pp.h3_index AND r{i + 1}.run_id = {rid}"
        for i, rid in enumerate(run_ids)
    )

    # cost_profile_id may be NULL — emit a JOIN that simply yields NULLs in
    # that case so COALESCE in the SELECT degrades cleanly to 0.
    cp_clause = (
        f"AND cpv.cost_profile_id = {cost_profile_id}"
        if cost_profile_id is not None
        else "AND FALSE"
    )

    return f"""
    DROP TABLE IF EXISTS bioprotect.{table_name};
    CREATE UNLOGGED TABLE bioprotect.{table_name} AS
    SELECT
      pp.h3_index::text                     AS puid,
      hc.geometry                           AS geometry,
      COALESCE(cpv.cost, 0)::float8         AS cost,
      COALESCE(cpv.status, 0)::int4         AS status,
      {run_cols},
      ({sum_expr})::int                     AS sel_freq,
      ROUND((({sum_expr}) * 100.0 / {n})::numeric, 1)::float8 AS sel_pct
    FROM bioprotect.project_pus pp
    JOIN bioprotect.h3_cells hc ON hc.h3_index = pp.h3_index
    LEFT JOIN bioprotect.cost_profile_values cpv
      ON cpv.project_pu_id = pp.id
      {cp_clause}
    {run_joins}
    WHERE pp.project_id = {project_id};
    """


async def _build_runs_csv(pg, run_ids):
    df = await pg.execute(
        """
        SELECT id AS run_id, label, description, created_at, status
        FROM bioprotect.prioritizr_runs
        WHERE id = ANY(%s)
        """,
        data=[run_ids],
        return_format="DataFrame",
    )
    # Preserve the caller's run order so column N matches row N.
    order_map = {rid: i + 1 for i, rid in enumerate(run_ids)}
    df["column"] = df["run_id"].map(order_map).map(lambda i: f"run_{i}")
    df = df.sort_values("column").reset_index(drop=True)
    return df[["column", "run_id", "label", "description", "status", "created_at"]]


async def _build_features_csv(pg, project_id, run_ids):
    """Mirrors prioritizr_handler.get_feature_representation, restricted to
    the given run set, and returns a tidy per-feature summary including
    runs-met / runs-total."""
    df = await pg.execute(
        """
        WITH totals AS (
            SELECT pfa.feature_unique_id,
                   SUM(pfa.amount) AS total_amount
            FROM   bioprotect.pu_feature_amounts pfa
            WHERE  pfa.project_id = %s
            GROUP  BY pfa.feature_unique_id
        ),
        per_run AS (
            SELECT rr.run_id,
                   pfa.feature_unique_id,
                   SUM(pfa.amount) AS represented_amount
            FROM   bioprotect.pu_feature_amounts pfa
            JOIN   bioprotect.prioritizr_run_results rr
                     ON  rr.h3_index = pfa.h3_index
                     AND rr.run_id = ANY(%s)
                     AND rr.solution = 1
            WHERE  pfa.project_id = %s
            GROUP  BY rr.run_id, pfa.feature_unique_id
        ),
        per_run_pct AS (
            SELECT pr.feature_unique_id,
                   pr.run_id,
                   CASE WHEN COALESCE(t.total_amount, 0) > 0
                        THEN (pr.represented_amount / t.total_amount * 100)
                        ELSE 0 END AS pct
            FROM   per_run pr
            LEFT JOIN totals t ON t.feature_unique_id = pr.feature_unique_id
        )
        SELECT
            pf.feature_unique_id,
            mif.alias                                       AS alias,
            pf.target_value                                 AS target_pct,
            ROUND(AVG(p.pct)::numeric, 2)                   AS achieved_avg_pct,
            COUNT(*) FILTER (WHERE p.pct >= pf.target_value) AS runs_met,
            %s                                              AS runs_total
        FROM   bioprotect.project_features pf
        JOIN   bioprotect.metadata_interest_features mif
                 ON mif.unique_id = pf.feature_unique_id
        LEFT JOIN per_run_pct p ON p.feature_unique_id = pf.feature_unique_id
        WHERE  pf.project_id = %s
        GROUP  BY pf.feature_unique_id, mif.alias, pf.target_value
        ORDER  BY mif.alias
        """,
        data=[project_id, run_ids, project_id, len(run_ids), project_id],
        return_format="DataFrame",
    )
    # Replace underscores in alias for the human-friendly CSV column.
    if "alias" in df.columns:
        df["alias"] = df["alias"].fillna("").str.replace("_", " ", regex=False)
    return df


async def _run_ogr2ogr(pg, table_name, out_path, fmt):
    """Streams the temp table out via ogr2ogr in either ESRI Shapefile or
    GPKG format. `out_path` is a folder for shapefile, a .gpkg file for gpkg."""
    cfg = pg.config
    pg_conn = (
        f'PG:"host={cfg.DATABASE_HOST} user={cfg.DATABASE_USER} '
        f'dbname={cfg.DATABASE_NAME} password={cfg.DATABASE_PASSWORD} '
        f'ACTIVE_SCHEMA=bioprotect"'
    )
    if fmt == "shp":
        cmd = (
            f'"{cfg.OGR2OGR_EXECUTABLE}" -f "ESRI Shapefile" "{out_path}" '
            f'{pg_conn} -sql "SELECT * FROM {table_name};" -nln pus '
            f'-t_srs EPSG:4326'
        )
    else:  # gpkg
        cmd = (
            f'"{cfg.OGR2OGR_EXECUTABLE}" -f "GPKG" "{out_path}" '
            f'{pg_conn} -sql "SELECT * FROM {table_name};" -nln pus '
            f'-t_srs EPSG:4326'
        )
    rc = await run_command(cmd)
    if rc != 0:
        raise ServicesError(f"ogr2ogr export failed (rc={rc})")


async def build_export_zip(pg, export_root, project_id, run_ids, fmt):
    """
    Builds the export and returns the absolute path of the resulting zip
    file. The caller is responsible for deleting the file and the
    surrounding work folder once it has been streamed to the client.

    Returns:
        (zip_path, work_folder)
    """
    if fmt not in ALLOWED_FORMATS:
        raise ServicesError(
            f"Unsupported format '{fmt}'. Expected one of: {ALLOWED_FORMATS}"
        )
    _validate_ids(project_id, run_ids)

    cost_profile_id = await _resolve_active_cost_profile_id(pg, project_id)

    # Unique names so simultaneous exports don't collide.
    uid = uuid.uuid4().hex[:12]
    table_name = f"export_{uid}"
    work_folder = os.path.join(export_root, f"run_export_{uid}")
    os.makedirs(work_folder, exist_ok=True)

    try:
        # 1. Build the temp table
        sql_text = _build_export_sql(table_name, project_id, run_ids, cost_profile_id)
        await pg.execute(sql_text)

        # 2. Export it via ogr2ogr
        if fmt == "shp":
            await _run_ogr2ogr(pg, table_name, work_folder, "shp")
        else:
            gpkg_path = os.path.join(work_folder, "pus.gpkg")
            await _run_ogr2ogr(pg, table_name, gpkg_path, "gpkg")

        # 3. Sidecar CSVs
        runs_df = await _build_runs_csv(pg, run_ids)
        runs_df.to_csv(os.path.join(work_folder, "runs.csv"), index=False)

        features_df = await _build_features_csv(pg, project_id, run_ids)
        features_df.to_csv(os.path.join(work_folder, "features.csv"), index=False)

        # 4. Zip everything in the work folder
        zip_basename = f"project_{project_id}_runs_{fmt}_{uid}"
        zip_path_no_ext = os.path.join(export_root, zip_basename)
        zip_folder(work_folder, zip_path_no_ext)
        zip_path = zip_path_no_ext + ".zip"

        if not os.path.exists(zip_path):
            raise ServicesError("Zip file was not produced")

        return zip_path, work_folder
    except Exception:
        # Best-effort cleanup on failure — work folder only; caller cleans
        # up the zip path on the success branch.
        shutil.rmtree(work_folder, ignore_errors=True)
        raise
    finally:
        # Always drop the temp table — it's UNLOGGED but still uses space.
        try:
            await pg.execute(f"DROP TABLE IF EXISTS bioprotect.{table_name};")
        except Exception as e:
            logging.warning("Failed to drop export temp table %s: %s", table_name, e)
