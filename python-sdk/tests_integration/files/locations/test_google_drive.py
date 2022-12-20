from unittest.mock import patch

import pytest
from googleapiclient.discovery import Resource

from astro.files.locations import create_file_location


@pytest.mark.integration
def test_get_transport_params_for_gdrive():  # skipcq: PYL-W0612, PTC-W0065
    """test get_transport_params() method which should return gdrive resource client"""
    path = "gdrive://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert isinstance(credentials["client"], Resource)


@pytest.mark.integration
@patch("astro.files.locations.google.gdrive._find_item_id")
def test_remote_object_exception(
    mock_folder_id,
):
    """Raise exception when gdrive filepath doesn't exists"""
    mock_folder_id.side_effect = FileNotFoundError()
    location = create_file_location("gdrive://data/ADOPTION_CENTER_1_unquoted.csv")
    with pytest.raises(FileNotFoundError):
        location.paths


@pytest.mark.integration
@patch("astro.files.locations.google.gdrive._find_item_id")
def test_size_exception(mock_folder_id):
    """Raise exception for a file not existing in gdrive filepath"""
    mock_folder_id.side_effect = FileNotFoundError()
    location = create_file_location("gdrive://data/ADOPTION_CENTER_1_unquoted.csv")
    with pytest.raises(FileNotFoundError):
        location.size
