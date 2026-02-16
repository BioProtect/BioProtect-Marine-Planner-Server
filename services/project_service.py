import datetime
from os import path

from services.service_error import ServicesError


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
