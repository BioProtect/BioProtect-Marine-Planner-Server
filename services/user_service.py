import glob
from os.path import basename, join, normpath
from types import SimpleNamespace

from classes.folder_path_config import get_folder_path_config
from services.file_service import (get_key_values_from_file, read_file,
                                   write_to_file)
from services.service_error import ServicesError

fp_config = get_folder_path_config()


def get_users():
    """
    Retrieves a list of all registered users from the users_folder directory.

    Returns:
        List[str]: A list of usernames.
    """
    # Get a list of folders in the users_folder
    user_folders = glob.glob(join(fp_config.USERS_FOLDER, "*/"))

    # Extract usernames from the folder paths
    users = [basename(normpath(folder)) for folder in user_folders]

    # Remove unwanted special folders
    excluded_folders = {"input", "output", "MarxanData", "MarxanData_unix"}
    users = [
        u for u in users if u not in excluded_folders and not u.startswith("_")]

    return users


def get_users_data(users):
    """
    Retrieves data for the given list of users.

    Args:
        users (List[str]): List of usernames.

    Returns:
        List[Dict[str, Any]]: List of dictionaries containing user data.
    """
    users.sort()
    users_data = []

    for user in users:
        user_folder = join(fp_config.USERS_FOLDER, user)
        tmp_obj = SimpleNamespace()
        tmp_obj.folder_user = user_folder

        # Retrieve the user data
        get_user_data(tmp_obj)

        # Add the user's data to the list
        user_data = tmp_obj.userData.copy()  # pylint:disable=no-member
        user_data.update({'user': user})
        users_data.append(user_data)

    return users_data


def get_user_data(obj, returnPassword=False):
    """Gets the data on the user from the user.dat file. These are set on the passed obj in the userData attribute.

    Args:
        obj (BaseHandler): The request handler instance.
        returnPassword (bool): Optional. Set to True to return the users password. Default value is False.
    Returns:
        None
    """
    user_data = get_key_values_from_file(join(obj.folder_user, "user.dat"))
    # set the userData attribute on this object
    if (returnPassword):
        obj.userData = user_data
    else:
        obj.userData = {key: value for key,
                        value in user_data.items() if key != 'PASSWORD'}


def get_notifications_data(obj):
    """
    Retrieves the notification data for a user.

    Args:
        obj (BaseHandler): The request handler instance.

    Returns:
        list: A list of the user's notification data, or an empty list if the file does not exist or is empty.
    """
    # Construct the path to the notifications file
    notifications_file_path = join(
        obj.folder_user, "notifications.dat")

    try:
        # Read the data from the notifications file
        data = read_file(notifications_file_path)
        return data.split(",") if data else []
    except FileNotFoundError:
        # Return an empty list if the file does not exist
        return []
    except Exception as e:
        # Log the error and return an empty list
        print(f"Error reading notifications file: {e}")
        return []


def dismiss_notification(obj, notificationid):
    """Appends the notification ID to the user's "notifications.dat" file to dismiss the notification.

    Args:
        obj (BaseHandler): The request handler instance.
        notificationid (int): The notification ID to be dismissed.

    Returns:
        None
    """
    # Validate notificationid
    if not isinstance(notificationid, int):
        raise ValueError("notificationid must be an integer.")

    # Get the current notification IDs from the file
    ids = get_notifications_data(obj)
    ids.append(str(notificationid))  # Append as a string for consistency
    write_to_file(obj.folder_user + "notifications.dat", ",".join(ids))


def reset_notifications(obj):
    """Resets all notification for the user by clearing the "notifications.dat".

    Args:
        obj (BaseHandler): The request handler instance.
    Returns:
        None
    """
    write_to_file(obj.folder_user + "notifications.dat", "")
