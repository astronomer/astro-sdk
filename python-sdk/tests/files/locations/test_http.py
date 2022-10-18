import pathlib

import pytest

from astro.constants import FileLocation
from astro.files.locations import create_file_location
from astro.files.locations.base import BaseFileLocation

CWD = pathlib.Path(__file__).parent

sample_filepaths_per_location = [
    (FileLocation.HTTP, "http://domain/some-file"),
    (FileLocation.HTTPS, "https://domain/some-file"),
]
sample_filepaths = [items[1] for items in sample_filepaths_per_location]
sample_filepaths_ids = [items[0].value for items in sample_filepaths_per_location]


@pytest.mark.parametrize(
    "expected_location,filepath",
    sample_filepaths_per_location,
    ids=sample_filepaths_ids,
)  # skipcq: PTC-W0065
def test_get_location_type_with_supported_location(expected_location, filepath):  # skipcq: PTC-W0065
    """test get_location_type() with all the supported locations"""
    location = BaseFileLocation.get_location_type(filepath)
    assert location == expected_location


@pytest.mark.parametrize("path", ["http://domain/file", "https://domain/file"], ids=["http", "https"])
def test_get_transport_params(path):  # skipcq: PYL-W0612, PTC-W0065
    """test get_transport_params() with API endpoint"""
    location = create_file_location(path)
    credentials = location.transport_params
    assert credentials is None


@pytest.mark.parametrize(
    "path",
    ["https://domain/some-file.txt", "http://domain/some-file.txt"],
    ids=["http", "https"],
)
def test_describe_get_paths(path):  # skipcq: PYL-W0612, PTC-W0065
    """test get_paths with API endpoint"""
    location = create_file_location(path)
    assert location.paths == [path]


@pytest.mark.integration
def test_size():
    """Test get_size() of for local file."""
    location = create_file_location(
        "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
    )
    assert location.size > 0
