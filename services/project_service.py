import datetime
import glob
import shutil
from os import path, sep, walk
import json

import pandas as pd
from classes.folder_path_config import get_folder_path_config
from classes.postgis_class import get_pg
from services.file_service import (get_key_value, get_key_values_from_file,
                                   get_keys, read_file, update_file_parameters)
from services.service_error import ExtendableObject, ServicesError
from services.user_service import get_users

fp_config = get_folder_path_config()


def copy_directory(src, dest):
    """Recursively copies a directory from src to dest."""
    try:
        shutil.copytree(src, dest)
    except (shutil.Error, OSError) as e:
        raise ServicesError(
            f"Failed to copy directory from '{src}' to '{dest}': {e}") from e


def set_folder_paths(obj, arguments, users_folder):
    """Sets the various paths to the user's folder and project folders using the request arguments in the passed object.

    Args:
        obj (BaseHandler): The request handler instance.
        arguments (dict): See https://www.tornadoweb.org/en/stable/httputil.html

    Returns:
        None
    """
    def decode_argument(arg_name):
        """Helper function to decode argument value."""
        return arguments[arg_name][0].decode("utf-8").strip()

    if "user" not in arguments:
        return

    # Set user-related paths
    user = decode_argument("user")
    obj.folder_user = path.join(users_folder, user) + sep
    obj.user = user

    # Check for project-related paths
    if "project" not in arguments:
        return

    project = decode_argument("project")
    obj.project_folder = path.join(obj.folder_user, project) + sep
    obj.input_folder = path.join(obj.project_folder, "input")
    obj.output_folder = path.join(obj.project_folder, "output") + sep
    if obj.get_argument("project", default=None) is not None:
        obj.project = obj.get_argument("project")

    return obj


def create_project(obj, name, template_folder):
    """Creates a new empty project with the passed parameters.

    Args:
        obj (BaseHandler): The request handler instance.
        name (str): The name of the project to create.

    Returns:
        None

    Raises:
        ServicesError: If the project already exists.
    """
    # Construct the full path for the new project
    project_path = path.join(obj.folder_user, name)

    # Check if the project already exists
    if path.exists(project_path):
        raise ServicesError(f"The project '{name}' already exists")

    # Copy the empty project template to the new project location
    copy_directory(template_folder, project_path)

    # Set the folder paths for the new project in the object
    set_folder_paths(obj, {
        'user': [obj.user.encode("utf-8")],
        'project': [name.encode("utf-8")]
    })


def get_safe_project_name(project_name):
    """Returns a safe name that can be used as a folder name by replacing spaces with underscores

    Args:
        project_name (string): Unsafe project name
    Returns:
        string: A safe project name
    """
    return project_name.strip().replace(" ", "_")


def delete_project(obj):
    """Deletes a project.

    Args:
        obj (BaseHandler): The request handler instance.
    Returns:
        None
    """
    project_folder = obj.project_folder

    # Validate that the project folder exists before attempting to delete it
    if not path.exists(project_folder):
        raise ServicesError(f"The project folder '{
                            project_folder}' does not exist.")

    try:
        shutil.rmtree(project_folder)
    except Exception as e:  # Catching all exceptions is a general approach
        raise ServicesError(f"Error deleting project folder: {e}") from e

    # Optionally, you could log the deletion
    print(f"Successfully deleted project folder: {project_folder}")


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


async def get_project_data(pg, obj):
    """Fetches the project data from the input.dat file as categorized settings
    (project, metadata, files, runParameters, and renderer) and sets it on the
    passed object in the projectData attribute.

    Args:
        obj (BaseHandler): The request handler instance.

    Returns:
        None
    """

    input_file_params = ["PUNAME", "SPECNAME",
                         "PUVSPRNAME", "BOUNDNAME", "BLOCKDEF"]
    run_params = ['BLM', 'PROP', 'RANDSEED', 'NUMREPS', 'NUMITNS', 'STARTTEMP', 'NUMTEMP', 'COSTTHRESH', 'THRESHPEN1', 'THRESHPEN2', 'SAVERUN', 'SAVEBEST', 'SAVESUMMARY',
                  'SAVESCEN', 'SAVETARGMET', 'SAVESUMSOLN', 'SAVEPENALTY', 'SAVELOG', 'RUNMODE', 'MISSLEVEL', 'ITIMPTYPE', 'HEURTYPE', 'CLUMPTYPE', 'VERBOSITY', 'SAVESOLUTIONSMATRIX']
    metadata_params = ['DESCRIPTION', 'CREATEDATE', 'PLANNING_UNIT_NAME',
                       'OLDVERSION', 'IUCN_CATEGORY', 'PRIVATE', 'COSTS']
    renderer_params = ['CLASSIFICATION', 'NUMCLASSES',
                       'COLORCODE', 'TOPCLASSES', 'OPACITY']

    params_array = []
    files_dict = {}
    metadata_dict = {}
    renderer_dict = {}

    # Load the input.dat file content
    input_file_path = path.join(obj.project_path, "input.dat")
    file_content = read_file(input_file_path)
    # Extract keys from the file content
    keys = get_keys(file_content)

    # Iterate over each key and categorize its value
    for key in keys:
        key_value = get_key_value(file_content, key)

        # Handle Input Files section of input.dat file
        if key in input_file_params:
            files_dict[key_value[0]] = key_value[1]

        # Handle run parameters section
        elif key in run_params:
            params_array.append({'key': key_value[0], 'value': key_value[1]})

         # Handle renderer section
        elif key in renderer_params:
            renderer_dict[key_value[0]] = key_value[1]

        # Handle metadata section
        elif key in metadata_params:
            metadata_dict[key_value[0]] = key_value[1]

            # If PLANNING_UNIT_NAME, fetch related metadata
            if key == 'PLANNING_UNIT_NAME':
                df = await pg.execute("SELECT * FROM bioprotect.get_planning_units_metadata(%s)",
                                      data=[key_value[1]],
                                      return_format="DataFrame")
                default_metadata = {
                    'pu_alias': key_value[1],
                    'pu_description': 'No description',
                    'pu_domain': 'Unknown domain',
                    'pu_area': 'Unknown area',
                    'pu_creation_date': 'Unknown date',
                    'pu_created_by': 'Unknown',
                    'pu_country': 'Unknown'
                }

                if not df.empty:
                    row = df.iloc[0]
                    metadata_dict.update({
                        'pu_alias': row.get('alias', key_value[1]),
                        'pu_description': row.get('description', 'No description'),
                        'pu_domain': row.get('domain', 'Unknown domain'),
                        'pu_area': row.get('area', 'Unknown area'),
                        'pu_creation_date': row.get('creation_date', 'Unknown date'),
                        'pu_created_by': row.get('created_by', 'Unknown'),
                        'pu_country': row.get('country', 'Unknown'),
                    })
                else:
                    metadata_dict.update(default_metadata)

                # Set the categorized data to the obj's projectData attribute
                obj.projectData = {
                    'project': obj.project,
                    'metadata': metadata_dict,
                    'files': files_dict,
                    'runParameters': params_array,
                    'renderer': renderer_dict
                }


def clone_a_project(source_folder, destination_folder):
    """Clones a project from the source folder to the destination folder.

    Args:
        source_folder (string): Full path to the source folder.
        destination_folder (string): Full path to the destination folder.

    Returns:
        string: The name of the cloned project.
    """
    # Get the original project name
    original_project_name = path.basename(path.normpath(source_folder))

    # Create the initial path for the new project folder
    new_project_folder = path.join(
        destination_folder, original_project_name) + sep

    # Ensure the new folder does not already exist, and append "_copy" if it does
    while path.exists(new_project_folder):
        new_project_folder = new_project_folder.rstrip(
            sep) + "_copy" + sep

    # Clone the project by copying the entire folder tree
    shutil.copytree(source_folder, new_project_folder)

    # Update the 'input.dat' file with new description and creation date
    update_file_parameters(
        path.join(new_project_folder, "input.dat"),
        {
            'DESCRIPTION': f"Clone of project '{original_project_name}'",
            'CREATEDATE': datetime.datetime.now().strftime("%d/%m/%y %H:%M:%S")
        }
    )

    # Return the name of the newly cloned project
    return path.basename(path.normpath(new_project_folder))


async def get_projects(obj, guest_username):
    """Gets the projects for the currently logged-on user.

    Args:
        obj (BaseHandler): The request handler instance.

    Returns:
        list[dict]: A list of dictionaries containing each project's data.
    """
    # Check if the user is a guest or an admin
    user_role = obj.get_secure_cookie("role").decode("utf-8")
    if obj.user == guest_username or user_role == "Admin":
        # If the user is a guest or admin, retrieve all projects
        obj.projects = await get_all_projects()
    else:
        # Otherwise, retrieve only the projects for the logged-in user
        obj.projects = await get_projects_for_user(obj.user)


async def get_all_projects():
    """Gets data for all projects.

    Returns:
        list[dict]: A list of dictionaries containing data for each project.
    """
    all_projects = []
    # Get a list of all users
    users = get_users()

    # Iterate through the users and gather project data for each user
    for user in users:
        user_projects = await get_projects_for_user(user)
        all_projects.extend(user_projects)

    return all_projects


async def get_projects_for_user(user):
    """Gets the projects for the specified user.

    Args:
        user (str): The name of the user.

    Returns:
        list[dict]: A list of dictionaries containing each project's data.
    """
    pg = await get_pg()
    # Get a list of project folders in the user's home directory
    project_folders = glob.glob(path.join(
        fp_config.USERS_FOLDER, user, "*/"))
    project_folders.sort()  # Sort the folders alphabetically

    projects = []
    tmp_obj = ExtendableObject()

    # Iterate through each project folder
    for project_dir in project_folders:
        # Extract the project name from the directory path
        project_name = path.basename(path.normpath(project_dir))

        # Skip system folders (folders beginning with "__")
        if not project_name.startswith("__"):
            # Set the project attributes on tmp_obj for further use
            tmp_obj.project = project_name
            tmp_obj.project_folder = path.join(
                fp_config.USERS_FOLDER, user, project_name, "")

            # Get the project data
            await get_project_data(pg, tmp_obj)

            # Append project data to the list
            projects.append({
                'user': user,
                'name': project_name,
                'description': tmp_obj.projectData["metadata"].get("DESCRIPTION", "No description"),
                'createdate': tmp_obj.projectData["metadata"].get("CREATEDATE", "Unknown"),
                'oldVersion': tmp_obj.projectData["metadata"].get("OLDVERSION", "Unknown"),
                'private': tmp_obj.projectData["metadata"].get("PRIVATE", False)
            })

    return projects


def get_projects_for_feature(feature_id, user_folder):
    """Gets a list of projects that contain the feature with the passed feature_id.

    Args:
        feature_id (str): The feature oid to search for.

    Returns:
        list[dict]: A list of projects where the feature is used, each containing 'user' and 'name'.
    """
    projects = []

    spec_dat_files = [path.join(root, f) for root, _, files in walk(
        user_folder) for f in files if f == "spec.dat"]

    for spec_file in spec_dat_files:
        # Load the CSV file into a dataframe
        if path.exists(spec_file):
            df = pd.read_csv(spec_file, sep=None, engine='python')
        else:
            continue

        # Check if the feature_id exists in the 'id' column of the dataframe
        if not feature_id in df['id'].values:
            continue

        # Extract project folder paths based on the file's location
        relative_path = path.relpath(spec_file, user_folder)
        prj_paths = relative_path.split(sep)

        if len(prj_paths) < 2:
            continue  # Skip invalid paths

        # Construct the project folder path
        prj_folder = path.join(user_folder, prj_paths[0], prj_paths[1])
        input_dat_path = path.join(prj_folder, "input.dat")
        values = get_key_values_from_file(input_dat_path)

        projects.append({
            'user': prj_paths[0],
            'name': prj_paths[1]
        })

    return projects
