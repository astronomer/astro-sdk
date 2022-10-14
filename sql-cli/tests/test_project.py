import tempfile

from sql_cli.project import BASE_SOURCE_DIR, Project
from tests.utils import list_dir

BASE_PATHS_COUNT = 19
BASE_PATHS = [obj for obj in list_dir(str(BASE_SOURCE_DIR)) if not obj.stem == ".gitkeep"]


def test_initialise_project_with_dirname():
    with tempfile.TemporaryDirectory() as dir_name:
        assert not list_dir(dir_name)
        Project(dir_name).initialise()
        new_dir_files_list = list_dir(dir_name)
        assert new_dir_files_list == BASE_PATHS
        assert len(new_dir_files_list) == BASE_PATHS_COUNT


def test_initialise_project_in_previously_initialised_dir():
    with tempfile.TemporaryDirectory() as dir_name:
        assert not list_dir(dir_name)
        Project(dir_name).initialise()
        new_dir_files_list = list_dir(dir_name)
        assert new_dir_files_list == BASE_PATHS
        assert len(new_dir_files_list) == BASE_PATHS_COUNT
        Project(dir_name).initialise()
        assert new_dir_files_list == BASE_PATHS
        assert len(new_dir_files_list) == BASE_PATHS_COUNT
        # TODO: make sure we did not override the content of existing files!
