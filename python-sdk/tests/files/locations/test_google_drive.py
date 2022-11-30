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
def test_remote_object(
    mock_folder_id,
    mock_get_conn,
):
    """with remote filepath"""
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
    """Test GoogleDriveHook hook is been called or not."""
    path = "gdrive://bucket/some-file"
    location = create_file_location(path)
    hook = location.hook
    assert isinstance(hook, GoogleDriveHook)


@patch("airflow.providers.google.suite.hooks.drive.GoogleDriveHook.get_conn")
def test_remote_object_not_found(
    mock_get_conn,
):
    """with remote filepath not found"""
    mock_get_conn.return_value.files.return_value.list.return_value.execute.return_value = {
        "files": [{"webContentLink": "ADOPTION_CENTER_1_unquoted.csv"}]
    }
    location = create_file_location("some random test")
    assert location.paths == []


@patch("astro.files.locations.google.gdrive._find_item_id")
def test_remote_object_exception(
    mock_folder_id,
):
    """with remote filepath doesn't exists"""
    mock_folder_id.side_effect = FileNotFoundError()
    location = create_file_location("gdrive://data/ADOPTION_CENTER_1_unquoted.csv")
    with pytest.raises(FileNotFoundError):
        location.paths


@patch("astro.files.locations.google.gdrive._find_item_id")
def test_size_exception(mock_folder_id):
    """When file not found when getting size"""
    mock_folder_id.side_effect = FileNotFoundError()
    location = create_file_location("gdrive://data/ADOPTION_CENTER_1_unquoted.csv")
    with pytest.raises(FileNotFoundError):
        location.size
