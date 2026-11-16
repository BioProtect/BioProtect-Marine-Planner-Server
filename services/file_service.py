import glob
import io
import re
import zipfile
from os import path, remove, sep, walk

from services.service_error import ServicesError


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

        # Count .shp files, not repeated extensions: ArcGIS exports ship both
        # name.shp.xml and name.xml, which is still one shapefile.
        shps = [f for f in filenames if f.lower().endswith('.shp')]
        if len(shps) > 1 and reject_multiple_shapefiles:
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

        # Name the shapefile after its .shp, not whatever file the zip lists
        # first (a map.png or readme first would give the wrong root).
        shps = [f for f in filenames if f.lower().endswith('.shp')]
        if not shps:
            raise ServicesError("The zip file does not contain a .shp file.")
        root_filename = path.splitext(shps[0])[0]
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

