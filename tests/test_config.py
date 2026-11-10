"""Guard: the app must see the test DB and temp folders, never the dev ones."""

from classes.db_config import DBConfig
from classes.folder_path_config import get_folder_path_config


def test_app_points_at_test_db_and_temp_folders():
    assert DBConfig().DATABASE_NAME == "bioprotect_test"
    paths = get_folder_path_config()
    for folder in (paths.IMPORT_FOLDER, paths.EXPORT_FOLDER):
        assert "bioprotect_tests_" in folder, folder
