from unittest.mock import patch

import pytest
from airflow.providers.google.suite.hooks.drive import GoogleDriveHook
from googleapiclient.discovery import Resource

from astro.files.locations import create_file_location


def test_get_transport_params_for_gdrive():  # skipcq: PYL-W0612, PTC-W0065
    """test get_transport_params() method which should return gdrive resource client"""
    path = "gdrive://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert isinstance(credentials["client"], Resource)


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


@patch("astro.files.locations.google.gdrive._find_item_id")
def test_remote_object_exception(
    mock_folder_id,
):
    """Raise exception when gdrive filepath doesn't exists"""
    mock_folder_id.side_effect = FileNotFoundError()
    location = create_file_location("gdrive://data/ADOPTION_CENTER_1_unquoted.csv")
    with pytest.raises(FileNotFoundError):
        location.paths


@patch("astro.files.locations.google.gdrive._find_item_id")
def test_size_exception(mock_folder_id):
    """Raise exception for a file not existing in gdrive filepath"""
    mock_folder_id.side_effect = FileNotFoundError()
    location = create_file_location("gdrive://data/ADOPTION_CENTER_1_unquoted.csv")
    with pytest.raises(FileNotFoundError):
        location.size


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
