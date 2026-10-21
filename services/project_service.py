import datetime
from os import path

from psycopg2 import sql

from services.service_error import ServicesError


async def get_project_crs(pg, project_id):
    """The project's EPSG code (projects.crs_epsg), or 4326 with no project.

    This is the CRS assumed for uploads that carry no CRS metadata.
    """
    # the frontend sends project_id=null / undefined when no project is open
    if not str(project_id or "").isdigit():
        return 4326
    rows = await pg.execute(
        "SELECT crs_epsg FROM bioprotect.projects WHERE id = %s",
        [int(project_id)], return_format="Array")
    return rows[0]["crs_epsg"] if rows else 4326


async def off_grid_warning(pg, project_id, crs_epsg, table=None, bbox=None):
    """A warning if data imported with an assumed CRS misses the project grid.

    Pass the imported `table` (geometry column `geometry`, EPSG:4326) or a
    `bbox` (west, south, east, north) in EPSG:4326. Returns None when it
    overlaps the grid, or when there's no project to check against.
    """
    if not str(project_id or "").isdigit():
        return None
    if table:
        footprint = sql.SQL("(SELECT ST_SetSRID(ST_Extent(geometry)::geometry, 4326) FROM bioprotect.{})").format(
            sql.Identifier(table))
        params = [int(project_id)]
    else:
        footprint = sql.SQL("ST_MakeEnvelope(%s, %s, %s, %s, 4326)")
        params = [*bbox, int(project_id)]
    rows = await pg.execute(
        sql.SQL("""
            SELECT ST_Intersects(mpu.envelope, {}) AS hit
            FROM bioprotect.projects p
            JOIN bioprotect.metadata_planning_units mpu
              ON mpu.unique_id = p.planning_unit_id
            WHERE p.id = %s
        """).format(footprint),
        params, return_format="Dict")
    if rows and rows[0]["hit"] is False:
        return (f"The file has no coordinate system, so it was read as "
                f"EPSG:{crs_epsg} (the project CRS), but it doesn't overlap "
                "the planning grid. If that's the wrong CRS, add one to the "
                "file (.prj or GeoTIFF CRS) and import it again.")
    return None


async def write_csv(obj, file_to_write, df, write_index=False):
    # sourcery skip: reintroduce-else, swap-if-else-branches, use-named-expression
    """Saves the dataframe to a CSV file as specified by the file_to_write argument.
    This only applies to files managed by Marxan in the input.dat file (e.g., SPECNAME, PUNAME, PUVSPRNAME, BOUNDNAME).

    Args:
        obj (BaseHandler): The request handler instance.
        file_to_write (str): The name of the input file as specified in the Input Files section of input.dat, e.g., INPUTDIR, PUNAME, SPECNAME, PUVSPRNAME, or BOUNDNAME.
        df (pd.DataFrame): The dataframe to write.
        write_index (bool): Optional. If True, writes the dataframe index to the file. Defaults to False.

    Returns:
        None

    Raises:
        ServicesError: If the filename is not set in the input.dat file.
    """
    file_name = obj.projectData["files"][file_to_write]
    if not file_name:  # Ensure the file has been created
        raise ServicesError(
            f"The filename for '{file_to_write}.dat' has not been set in the input.dat file.")

    df.to_csv(path.join(obj.input_folder, file_name), index=write_index)


def custom_serializer(obj):
    if isinstance(obj, datetime):
        return obj.isoformat()  # Convert datetime to string
    raise TypeError("Type not serializable")
