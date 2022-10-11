import os
import tempfile
from pathlib import Path
from typing import List

from sql_cli.project import BASE_SOURCE_DIR, Project


def ls(dir_name: str) -> List[Path]:
    """
    Return sorted list of files and directories available in the given directory.

    :param dir_name: Source directory name

    :returns: Sorted list of files and directories within the given directory.

    """
    return sorted([path.relative_to(dir_name) for path in Path(dir_name).rglob("*")])


def test_initialise_project_with_dirname():
    with tempfile.TemporaryDirectory() as dir_name:
        assert not os.listdir(dir_name)
        Project().initialise(target_dir=dir_name)
        new_dir_files_list = ls(dir_name)
        src_dir_files_list = ls(BASE_SOURCE_DIR)
        assert new_dir_files_list == src_dir_files_list
        assert len(new_dir_files_list) == 6


def initialise_project_in_previously_initialised_dir():
    with tempfile.TemporaryDirectory() as dir_name:
        assert not os.listdir(dir_name)
        Project().initialise(target_dir=dir_name)
        new_dir_files_list = ls(dir_name)
        src_dir_files_list = ls(BASE_SOURCE_DIR)
        assert new_dir_files_list == src_dir_files_list
        assert len(new_dir_files_list) == 6
        Project().initialise(target_dir=dir_name)
        assert new_dir_files_list == src_dir_files_list
        assert len(new_dir_files_list) == 6
