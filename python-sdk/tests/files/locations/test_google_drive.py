from unittest.mock import patch

import pytest
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook

from astro.files.locations import create_file_location


@patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
@patch("astro.files.locations.google.gdrive._find_item_id")
def test_get_paths_from_gdrive(
    mock_folder_id,
    mock_get_conn,
):
    """Get the list of files from the gdrive path"""
    mock_get_conn.return_value.files.return_value.list.return_value.execute.return_value = {
        "files": [{"webContentLink": "ADOPTION_CENTER_1_unquoted.csv"}]
    }
    mock_folder_id.return_value = "root"
    location = create_file_location("gdrive://data/ADOPTION_CENTER_1_unquoted.csv")
    assert sorted(location.paths) == sorted(["ADOPTION_CENTER_1_unquoted.csv"])


@patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
@patch("astro.files.locations.google.gdrive._find_item_id")
def test_size(mock_folder_id, mock_get_conn):
    """Test get_size() of for Google Drive file."""
    mock_get_conn.return_value.files.return_value.list.return_value.execute.return_value = {
        "files": [{"size": "110"}]
    }
    mock_folder_id.return_value = "root"
    path = "gdrive://data/ADOPTION_CENTER_1_unquoted.csv"
    location = create_file_location(path)
    assert location.size == 110


def test_hook():
    """Test whether GoogleDriveHook is being called or not."""
    path = "gdrive://bucket/some-file"
    location = create_file_location(path)
    hook = location.hook
    assert isinstance(hook, GoogleDriveHook)


@patch("astro.files.locations.google.gdrive.GdriveLocation._get_rel_path_parts")
def test_gdrive_file_not_found(
    mock_get_path,
):
    """Test when no file is found in gdrive path"""
    mock_get_path.return_value = []
    location = create_file_location("gdrive://bucket/some-file")
    assert location.paths == []


@patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
@patch("astro.files.locations.google.gdrive._find_item_id")
def test_size_key_error(mock_folder_id, mock_get_conn):
    """Raise exception when gdrive list doesn't return size attribute"""
    mock_get_conn.return_value.files.return_value.list.return_value.execute.return_value = {
        "files": [{"sizes": "110"}]
    }
    mock_folder_id.return_value = "root"
    path = "gdrive://data/ADOPTION_CENTER_1_unquoted.csv"
    location = create_file_location(path)
    with pytest.raises(IsADirectoryError):
        location.size
