# Project

1.  load input.dat file - (file is a .dat file with key values separated by space)
2.  get keys and values from file
3.  Loop through the params (see params below) and add the relevant key value to the relevant paramater data structure.
4.  files, meta, and renderer were dicts. run params was an array of dicts.
5.  Meta data is queried from the database and added to the following dict.
    If there is a `PLANNING_UNIT_NAME` key in the input.dat file, get the associated metadata from the database.

    ```python
    if key == "PLANNING_UNIT_NAME":
        df = await pg.execute("SELECT * FROM bioprotect.get_planning_units_metadata(%s)", data=[key_value[1]], return_format="DataFrame")
    ```

    The planningn unit metadata is in the following format:

    ```json
    default_metadata = {
        "pu_alias": key_value[1],
        "pu_description": "No description",
        "pu_domain": "Unknown domain",
        "pu_area": "Unknown area",
        "pu_creation_date": "Unknown date",
        "pu_created_by": "Unknown",
        "pu_country": "Unknown"
    }
    ```

6.  Return the obj with the attached data

So the `input.dat` file has the values for the below parameters.

```json
{
  "id": "project_example",
  "name": "Project Example",
  "description": "This is an example project",
  "input_file_params": [
    "PUNAME",
    "SPECNAME",
    "PUVSPRNAME",
    "BOUNDNAME",
    "BLOCKDEF"
  ],
  "run_params": [
    "BLM",
    "PROP",
    "RANDSEED",
    "NUMREPS",
    "NUMITNS",
    "STARTTEMP",
    "NUMTEMP",
    "COSTTHRESH",
    "THRESHPEN1",
    "THRESHPEN2",
    "SAVERUN",
    "SAVEBEST",
    "SAVESUMMARY",
    "SAVESCEN",
    "SAVETARGMET",
    "SAVESUMSOLN",
    "SAVEPENALTY",
    "SAVELOG",
    "RUNMODE",
    "MISSLEVEL",
    "ITIMPTYPE",
    "HEURTYPE",
    "CLUMPTYPE",
    "VERBOSITY",
    "SAVESOLUTIONSMATRIX"
  ],
  "metadata_params": [
    "DESCRIPTION",
    "CREATEDATE",
    "PLANNING_UNIT_NAME",
    "OLDVERSION",
    "IUCN_CATEGORY",
    "PRIVATE",
    "COSTS"
  ],
  "renderer_params": [
    "CLASSIFICATION",
    "NUMCLASSES",
    "COLORCODE",
    "TOPCLASSES",
    "OPACITY"
  ]
}
```
