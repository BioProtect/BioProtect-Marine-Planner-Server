import csv
import glob
import io
import re
import zipfile
from operator import itemgetter
from os import pardir, path, remove, sep, walk
from subprocess import PIPE

import numpy as np
import pandas as pd
from services.service_error import ServicesError
import shutil
from osgeo import ogr


def copy_directory(src, dest):
    """Recursively copies a directory from src to dest."""
    try:
        shutil.copytree(src, dest)
    except (shutil.Error, OSError) as e:
        raise ServicesError(
            f"Failed to copy directory from '{src}' to '{dest}': {e}") from e


def get_shapefile_fieldnames(shapefile):
    """
    Retrieves the field names from a shapefile.

    Args:
        shapefile (str): The full path to the shapefile (*.shp).

    Returns:
        list[str]: A list of field names in the shapefile.

    Raises:
        ServicesError: If the shapefile does not exist, cannot be opened, or contains no layers.
    """
    # Ensure OGR exceptions are raised
    ogr.UseExceptions()

    try:
        # Open the shapefile
        data_source = ogr.Open(shapefile)
        if not data_source:
            raise ServicesError(
                f"Shapefile '{shapefile}' not found or could not be opened.")

        # Access the first layer
        layer = data_source.GetLayer(0)
        if not layer:
            raise ServicesError(f"No layers found in shapefile '{shapefile}'.")

        # Extract field names from the layer definition
        layer_definition = layer.GetLayerDefn()
        return [layer_definition.GetFieldDefn(i).GetName() for i in range(layer_definition.GetFieldCount())]

    except RuntimeError as e:
        raise ServicesError(
            f"Error reading shapefile '{shapefile}': {e.args[0]}")


def normalize_dataframe(df, column_to_normalize_by, puid_column_name, classes=None):
    # sourcery skip: extract-method
    """
    # sourcery skip: extract-method
    Converts a DataFrame with duplicate values into a normalized array.

    Args:
        df (pd.DataFrame): The DataFrame to normalize.
        column_to_normalize_by (str): The column in the DataFrame used to provide the headings for the normalized data (e.g., "Status" column produces 1,2,3).
        puid_column_name (str): The name of the planning grid unit ID column to create the array of values.
        classes (int, optional): Number of classes to classify the data into. Defaults to None.

    Returns:
        list: The normalized data from the DataFrame organized as a list of values (headings) each with a list of PUIDs, e.g., [32, 2374, 5867, 24967...].
    """
    if df.empty:
        return []

    if classes:
        # Calculate the range and bin size for classification
        min_value = df[column_to_normalize_by].min()
        max_value = df[column_to_normalize_by].max()

        # Handle case where all values in the column are the same
        num_classes = 1 if min_value == max_value else classes
        bin_size = (max_value + 1 - min_value) / num_classes

        # Initialize bins
        bins = [[min_value + bin_size * (i + 1), []]
                for i in range(num_classes)]

        # Classify rows into bins
        for idx, row in df.iterrows():
            bin_index = int(
                (row[column_to_normalize_by] - min_value) / bin_size)
            bins[bin_index][1].append(int(row[puid_column_name]))

        return bins, min_value, max_value

    # Group by unique values in the column and organize data
    groups = df.groupby(column_to_normalize_by)
    return [
        [group, group_df[puid_column_name].tolist()]
        for group, group_df in groups
        if group != 0
    ]


def file_to_df(file_name):
    """Reads a file and returns the data as a DataFrame

    Args:
        file_name (string): The name of the file to read.
    Returns:
        DataFrame: The data from the file.
    """
    return pd.read_csv(file_name, sep=None, engine='python') if path.exists(file_name) else pd.DataFrame()


def get_dict_value(_dict, key):
    """Gets the value for a given key from a dictionary.

    Args:
        _dict (dict): The dictionary to search.
        key (str): The key whose value is to be retrieved.

    Returns:
        string: The value associated with the key.

    Raises:
        ServicesError: If the key does not exist.
    """
    try:
        return _dict[key]
    except KeyError:
        raise ServicesError(
            f"The key '{key}' does not exist in the dictionary")


def get_keys(s):
    """Extracts all keys from a string containing KEY/VALUE pairs.

    Args:
        s (str): The string to extract all keys from.

    Returns:
        list[str]: A list of keys extracted from the string.
    """
    # Match keys that start after a newline and consist of uppercase letters, digits, and underscores
    # This regex matches both Unix (\n) and Windows (\r\n) line endings.
    return re.findall(r'\n([A-Z0-9_]+)', s)


def get_key_value(text, parameterName):
    """Gets the key value combination from the text, e.g. PUNAME pu.dat returns ("PUNAME", "pu.dat")

    Args:
        text (str): The text to search for the key/value pair.
        parameterName (str): The key to search for.

    Returns:
        tuple: A tuple containing the parameter name and its corresponding value.
    """
    # Find the start index of the parameterName
    start_idx = text.find(parameterName)

    if start_idx == -1:
        raise ValueError(f"Parameter '{parameterName}' not found in text.")

    # Determine the end of the line, considering both \r\n and \n
    end_idx = text.find('\n', start_idx)
    end_idx_crlf = text.find('\r\n', start_idx)

    # Choose the closest end index
    if end_idx == -1 and end_idx_crlf == -1:
        end_idx = len(text)  # End of text if no line ending is found
    elif end_idx == -1:
        end_idx = end_idx_crlf
    elif end_idx_crlf == -1:
        end_idx = end_idx
    else:
        end_idx = min(end_idx, end_idx_crlf)

    # Extract the value between the parameter name and the line ending
    # +1 for the space or separator
    value_start_idx = start_idx + len(parameterName) + 1
    value = text[value_start_idx:end_idx].strip()  # Strip whitespace

    # Convert text representations of booleans to actual booleans
    if value == "True":
        value = True
    elif value == "False":
        value = False

    return parameterName, value


def read_file(filename):
    for encoding in ("utf-8", "ISO-8859-1"):
        try:
            with io.open(filename, mode="r", encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue  # Try the next encoding if decoding fails

    # If we exhaust all encoding attempts, re-raise the last IOError
    raise IOError(f"Unable to read file {filename} with supported encodings.")


def get_key_values_from_file(filename):
    """Gets the key/value pairs from a text file as a dictionary.

    Args:
        filename (str): Full path to the file that will be read.

    Returns:
        dict: The key/value pairs as a dictionary.

    Raises:
        ServicesError: If the file does not exist or cannot be read.
    """
    if not path.exists(filename):
        raise ServicesError(f"The file '{filename}' does not exist")

    try:
        # Get the file contents
        file_content = read_file(filename)
    except Exception as e:
        raise ServicesError(f"Error reading file '{filename}': {str(e)}")

    # Initialize dictionary to hold key-value pairs
    data = {}

    # Get the keys from the file content
    keys = get_keys(file_content)

    # Populate the dictionary with key-value pairs
    for key in keys:
        key, value = get_key_value(file_content, key)
        # Add key-value to the dictionary
        data[key] = value

    return data


def write_to_file(filename, data, mode='w'):
    print('Writing to file - filename, mode: ', filename, mode)
    if 'b' in mode:
        with open(filename, mode) as f:
            f.write(data)
    else:
        with open(filename, mode, encoding='utf-8') as f:
            f.write(data)


def update_file_parameters(filename, newParams):
    """Updates the parameters in the file with the new parameters. The parameters with the keys that match those in newParams are updated, while all other parameters are left unchanged.

    Args:
        filename (str): Full path to the file that will be updated.
        newParams (dict): A dictionary of the key/value pairs that will be updated.
    Returns:
        None
    """
    if not newParams:
        return  # No parameters to update

    # Read the existing parameters
    file_content = read_file(filename)

    # Prepare a list of updated lines
    updated_lines = []

    # Split the file content into lines and process each line
    for line in file_content.splitlines():
        # Check if the line starts with any key in newParams
        updated_line = line
        for k, v in newParams.items():
            if line.startswith(k):
                # Update the parameter
                updated_line = f"{k} {v}"  # Updating the parameter value
                break  # No need to check further keys if one is matched
        updated_lines.append(updated_line)

    # Write the updated lines back to the file
    write_to_file(filename, "\n".join(updated_lines))


def add_parameter_to_file(file_type, param_key, value, user_folder, app_folder):
    """Creates a new parameter in the *.dat file, either user (user.dat), project (input.dat) or server (server.dat), by iterating through all the files and adding the key/value if it doesnt already exist.

    Args:
        _type (string):
            The type of configuration file to add the parameter to. One of server, user or project.
        key (string):
            The key to create/update.
        value (string):
            The value to set.
    Returns:
        string[]: A list of the files that were updated.
    """
    results = []

    # Map configuration types to file search patterns
    config_file_patterns = {
        'user': user_folder + "*" + sep + "user.dat",
        'project': user_folder + "*" + sep + "*" + sep + "input.dat",
        'server': app_folder + "server.dat"
    }

    # Get the appropriate file pattern based on the config type
    file_pattern = config_file_patterns.get(file_type.lower())

    if not file_pattern:
        raise ValueError(
            "Invalid configuration type. Must be 'server', 'user', or 'project'.")

    # Fetch all matching files
    config_files = glob.glob(file_pattern)

    # iterate through the files and add the keys if necessary
    for file in config_files:
        content = read_file(file)
        existing_keys = get_keys(content)

        if param_key not in existing_keys:
            # Append the new param_key-value pair
            content += f"{param_key} {value}\n"
            write_to_file(file, content)
            logging.warning(f"Key '{param_key}' added to {file}")
            results.append(f"Key '{param_key}' added to {file}")
        else:
            # Update the existing param_key-value pair
            update_file_parameters(file, {param_key: value})
            logging.warning(
                f"Key '{param_key}' updated to '{value}' in {file}")
            results.append(f"Key '{param_key}' updated to '{value}' in {file}")

    return results


def write_df_to_file(file, dataframe):
    """Writes the dataframe to the file. If the file exists, it appends the new data.
    If the file does not exist, it creates a new file.

    Args:
        file (str): The full path to the file that will be written.
        dataframe (pd.DataFrame): The dataframe to write.
    Returns:
        None
    Raises:
        ValueError: If the dataframe is empty.
        Exception: For any other error during file operations.
    """
    # Validate input
    if dataframe.empty:
        raise ValueError("The dataframe is empty. No data to write.")

    # Determine if the file exists
    if path.exists(file):
        # Read the current data, using a flexible separator
        df_existing = pd.read_csv(file, sep=None, engine='python')
        # Append new records
        df_combined = pd.concat([df_existing, dataframe], ignore_index=True)
    else:
        # If the file doesn't exist, use the new dataframe
        df_combined = dataframe.copy()

    # Write the combined dataframe to the file
    df_combined.to_csv(file, index=False)


def delete_all_files(folder):
    """Deletes all files in the specified folder, excluding files with the .dat extension.

    Args:
        folder (str): The full path to the folder where files will be deleted.

    Returns:
        None

    Raises:
        ValueError: If the specified folder does not exist or is not a directory.
    """
    # Validate the folder path
    if not path.exists(folder):
        raise ValueError(f"The folder '{folder}' does not exist.")
    if not path.isdir(folder):
        raise ValueError(f"The path '{folder}' is not a directory.")

    # Find all files in the specified folder
    # Use path.join for compatibility
    files = glob.glob(path.join(folder, "*"))

    # Delete files excluding those with .dat extension
    for f in files:
        if not f.endswith('.dat'):  # More explicit check
            try:
                remove(f)
                print(f"Deleted file: {f}")
            except Exception as e:
                print(f"Error deleting file '{f}': {e}")


def get_output_file(filename):  # sourcery skip: use-named-expression
    """
    Gets the correct filename+extension (normally either csv or txt) of a Marxan file. The extension of the Marxan output files depends on the settings SAVE* in the input.dat file and on the version of marxan.

    Args:
        filename (string): The full path to the file to get the correct filename for.
    Returns:
        The full path to the file with the correct extension.
    Raises:
        ServicesError: If the file does not exist.
    """
    files = glob.glob(filename + ".*")
    # If any files exist, return the first found with its extension
    if files:
        return files[0]  # Returns the full path including the extension

    # If no files found, raise an error
    raise ServicesError(f"The output file '{filename}' does not exist.")


def create_zipfile(folder, feature_class_name):
    """Creates a zip file from all the files that have the root name feature_class_name in the folder.

    Args:
        folder (string): The full path to the folder with the files that will be matched.
        feature_class_name (string): The root filename that will match the files in folder that will be added to the zip file.
    Returns:
        string: The full path to the zip file.
    """
    # get the matching files
    folder = folder if folder.endswith(sep) else folder + sep
    # Get all files matching the base feature class name
    matched_files = glob.glob(folder + feature_class_name + '.*')

    # get the zip filename
    zip_filename = folder + feature_class_name + ".zip"
    with zipfile.ZipFile(zip_filename, 'w') as myzip:
        for file in matched_files:
            arcname = path.basename(file)
            myzip.write(file, arcname)
    # Optionally delete the archive files after zipping
    delete_archive_files(folder, feature_class_name)

    return zip_filename


def delete_archive_files(folder, archivename):
    """Deletes all files matching the archivename in the folder. This is typically used to delete constituent files from a zipped shapefile after extraction.

    Args:
        folder (string): The full path to the folder containing the files.
        archivename (string): The base name of the files to delete (files sharing this root name will be deleted).

    Returns:
        None
    """
    # List of file extensions to be deleted
    valid_extensions = ['shx', 'shp', 'xml', 'sbx', 'prj', 'sbn',
                        'dbf', 'cpg', 'qpj', 'SHX', 'SHP', 'XML',
                        'SBX', 'PRJ', 'SBN', 'DBF', 'CPG', 'QPJ']

    # Get all matching files in the folder
    matching_files = glob.glob(path.join(folder, archivename) + '.*')

    # Delete files with valid extensions
    for file in matching_files:
        extension = file.split('.')[-1]
        if extension in valid_extensions:
            remove(file)


def delete_zipped_shapefile(folder, zipfile, archivename):
    """Deletes a zip file and the archive files, e.g. deleteZippedShapefile(PROJECT_FOLDER, "pngprovshapes.zip","pngprov")

    Args:
        folder (string): The full path to the folder which contains the zip file and/or the individual files.
        zipfile (string): The name of the zip file that will be deleted.
        archivename (string): The root filename that will match the files in folder that will be deleted.
    Returns:
        None
    """
    # delete any archive files
    folder = path.join(folder, '')

    delete_archive_files(folder, archivename)
    zip_path = path.join(folder, zipfile)
    if zipfile and path.exists(zip_path):
        remove(zip_path)


def zip_folder(folder, zip_file):
    """Creates a zip file from a folder and all its subfolders, saving the archive with relative paths.

    Args:
        folder (string): The full path to the folder that will be zipped.
        zip_file (string): The name of the zip file to create (without extension).

    Returns:
        None
    """
    zip_path = zip_file + '.zip'
    folder_length = len(folder) + 1

    with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipobj:
        for base, dirs, files in walk(folder):
            for file in files:
                file_path = path.join(base, file)
                arcname = path.relpath(file_path, folder)
                zipobj.write(file_path, arcname)


def check_zipped_shapefile(shapefile):
    # sourcery skip: use-named-expression
    """
    Validates that all necessary files for the shapefile are present. Raises an exception if any are missing.

    Args:
        shapefile (str): The full path to the shapefile (*.shp).

    Returns:
        None

    Raises:
        ServicesError: If the shapefile or any required supporting files are missing.
    """
    # Required extensions for a valid shapefile
    required_extensions = ['shp', 'shx', 'dbf']
    # Get the base name of the shapefile (without extension) and check if any files of that name exist
    base_name = shapefile.rsplit('.', 1)[0]

    if not glob.glob(f"{base_name}.*"):
        raise ServicesError(f"The shapefile '{shapefile}' was not found.")

    # Check for missing required files
    missing_files = [
        ext for ext in required_extensions if not path.exists(f"{base_name}.{ext}")]

    if missing_files:
        missing_str = ", ".join(f"*.{ext}" for ext in missing_files)
        raise ServicesError(
            f"The following files are missing: {missing_str}."
        )


def get_files_in_folder(folder, filename):
    """Gets an array of filenames in the folder recursively.

    Args:
        folder (str): The full path to the folder to search recursively.
        filename (str): The filename to search for.

    Returns:
        list[str]: A list of the full filenames of the found files.
    """
    found_files = []

    # Use path.join for path concatenation
    for root, _, files in walk(folder):
        # Check if the specified filename is in the current directory's files
        if filename in files:
            found_files.append(path.join(root, filename))

    return found_files


def unzip_file(folder, filename):
    """Unzips a zip file.

    Args:
        folder (str): The full path to the folder with the zip file.
        filename (str): The name of the zip file that will be unzipped.

    Returns:
        list[str]: The list of filenames in the zip file that was unzipped.

    Raises:
        ServicesError: If the zip file does not exist or if extraction fails.
    """
    zip_path = path.join(folder, filename)

    # Check if the zip file exists
    if not path.exists(zip_path):
        raise ServicesError(f"The zip file '{filename}' does not exist.")

    try:
        # Create an instance of the zip file and extract all files
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(folder)
            return zip_ref.namelist()
    except zipfile.BadZipFile:
        raise ServicesError(f"The file '{filename}' is not a valid zip file.")
    except Exception as e:
        raise ServicesError(
            f"An error occurred while unzipping '{filename}': {str(e)}")


def unzip_shapefile(folder, filename, reject_multiple_shapefiles=True, search_term=None):
    """Unzips a zipped shapefile.

    Args:
        folder (str): The full path to the folder with the zip file.
        filename (str): The name of the zip file that will be unzipped.
        reject_multiple_shapefiles (bool): Optional. If True, raises an exception if there are multiple shapefiles in the zip file. Defaults to True.
        search_term (str): Optional. Filters the members of the zip file for those that match the search term, e.g., if search_term = 'polygons' it will extract all members whose filename contains the text 'polygon'. Defaults to None.

    Returns:
        str: The root filename of the first matching file that is unzipped, excluding the extension.

    Raises:
        ServicesError: If the zip file does not exist, contains multiple shapefiles, or does not meet the specified criteria.
    """
    zip_path = path.join(folder, filename)

    # Check if the zip file exists
    if not path.exists(zip_path):
        raise ServicesError(f"The zip file '{filename}' does not exist.")

    with zipfile.ZipFile(zip_path, 'r') as zip_ref:
        # Get the filenames, ignoring any duplicates in macOS archive files
        filenames = [f for f in zip_ref.namelist(
        ) if not f.startswith('__MACOSX')]

        # Check for multiple shapefiles
        extensions = [path.splitext(f)[1] for f in filenames]
        if len(extensions) != len(set(extensions)) and reject_multiple_shapefiles:
            raise ServicesError("The zip file contains multiple shapefiles.")

        # Filter files based on the search term, if specified
        if search_term:
            filenames = [f for f in filenames if search_term in f]
            if not filenames:
                raise ServicesError(
                    f"There were no files in the zip file matching the text '{search_term}'.")

        # Ensure there are no nested directories
        if any(sep in f for f in filenames):
            raise ServicesError(
                "The zipped file should not contain directories.")

        # Extract files
        root_filename = path.splitext(filenames[0])[0]
        try:
            if search_term:
                for f in filenames:
                    zip_ref.extract(f, folder)
            else:
                zip_ref.extractall(folder)
        except OSError:
            # Clean up extracted files in case of an error
            for f in filenames:
                file_path = path.join(folder, f)
                if path.exists(file_path):
                    remove(file_path)
            raise ServicesError(
                f"No space left on device extracting the file '{root_filename}'.")

    return root_filename


def delete_records_in_text_file(filename, id_column_name, ids):
    """Deletes records in a CSV file that have ID values matching the provided IDs.

    Args:
        filename (str): Full path to the file that will be processed.
        id_column_name (str): The column name containing IDs to match against.
        ids (list[int]): A list of integer values representing feature IDs.

    Returns:
        None

    Raises:
        ServicesError: If the file does not exist or cannot be processed.
    """
    if not path.exists(filename):
        raise ServicesError(f"The file '{filename}' does not exist.")

    # Load existing data from the CSV file
    df = pd.read_csv(filename, sep=None, engine='python') if path.exists(
        filename) else pd.DataFrame()

    # Remove records with matching IDs
    df_filtered = df[~df[id_column_name].isin(ids)]

    # Write the filtered results back to the file without including the index
    df_filtered.to_csv(filename, index=False)
