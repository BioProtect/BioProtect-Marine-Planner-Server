import argparse
import asyncio
from services.file_service import read_file, get_keys, get_key_value
from classes.postgis_class import get_pg


async def upload_input_dat(pg, project_id: int, input_dat_path: str):
    file_content = read_file(input_dat_path)
    keys = get_keys(file_content)

    input_file_params = ["PUNAME", "SPECNAME",
                         "PUVSPRNAME", "BOUNDNAME", "BLOCKDEF"]
    run_params = ['BLM', 'PROP', 'RANDSEED', 'NUMREPS', 'NUMITNS', 'STARTTEMP', 'NUMTEMP', 'COSTTHRESH', 'THRESHPEN1', 'THRESHPEN2', 'SAVERUN', 'SAVEBEST', 'SAVESUMMARY',
                  'SAVESCEN', 'SAVETARGMET', 'SAVESUMSOLN', 'SAVEPENALTY', 'SAVELOG', 'RUNMODE', 'MISSLEVEL', 'ITIMPTYPE', 'HEURTYPE', 'CLUMPTYPE', 'VERBOSITY', 'SAVESOLUTIONSMATRIX']
    metadata_params = ['DESCRIPTION', 'CREATEDATE', 'PLANNING_UNIT_NAME',
                       'OLDVERSION', 'IUCN_CATEGORY', 'PRIVATE', 'COSTS']
    renderer_params = ['CLASSIFICATION', 'NUMCLASSES',
                       'COLORCODE', 'TOPCLASSES', 'OPACITY']
    metadata_update = {}

    for key in keys:
        param, value = get_key_value(file_content, key)

        if key in input_file_params:
            await pg.execute("""
                INSERT INTO bioprotect.project_files (project_id, file_type, file_name)
                VALUES (%s, %s, %s)
            """, [project_id, param, value])

        elif key in run_params:
            await pg.execute("""
                INSERT INTO bioprotect.project_run_parameters (project_id, key, value)
                VALUES (%s, %s, %s)
            """, [project_id, param, str(value)])

        elif key in renderer_params:
            await pg.execute("""
                INSERT INTO bioprotect.project_renderer (project_id, key, value)
                VALUES (%s, %s, %s)
            """, [project_id, param, str(value)])

        # Collect metadata values for a single UPDATE
        elif key in metadata_params:
            if param == "DESCRIPTION":
                metadata_update["description"] = value
            elif param == "OLDVERSION":
                metadata_update["old_version"] = value == "True"
            elif param == "IUCN_CATEGORY":
                metadata_update["iucn_category"] = value
            elif param == "PRIVATE":
                metadata_update["is_private"] = value == "True"
            elif param == "COSTS":
                metadata_update["costs"] = value
            elif param == "PLANNING_UNIT_NAME":
                result = await pg.execute("""
                    SELECT unique_id FROM bioprotect.metadata_planning_units
                    WHERE feature_class_name = %s
                """, [value], return_format="Dict")
                if result:
                    metadata_update["planning_unit_id"] = result[0]["unique_id"]
                else:
                    print(
                        f"[!] Warning: '{value}' not found in metadata_planning_units")

            # Run a single update if there's anything to update
            if metadata_update:
                columns = ", ".join(
                    [f"{k} = ${i+1}" for i, k in enumerate(metadata_update)])
                values = list(metadata_update.values())
                values.append(project_id)  # For WHERE clause

                await pg.execute(
                    f"UPDATE bioprotect.projects SET {columns} WHERE id = ${len(values)}",
                    values
                )

    print(f"✅ input.dat for project ID {project_id} uploaded successfully.")


def main():
    parser = argparse.ArgumentParser(
        description="Upload input.dat to database.")
    parser.add_argument("--project-id", type=int, required=True,
                        help="ID of the project in 'projects' table.")
    parser.add_argument("--input-dat", type=str,
                        required=True, help="Path to input.dat file.")

    args = parser.parse_args()

    async def runner():
        pg = await get_pg()
        await upload_input_dat(pg, args.project_id, args.input_dat)

    asyncio.run(runner())


if __name__ == "__main__":
    main()
