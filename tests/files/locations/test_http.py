import pathlib

import pytest

from astro.constants import FileLocation
from astro.files.locations import location_factory
from astro.files.locations.base import Location

CWD = pathlib.Path(__file__).parent

sample_filepaths_per_location = [
    (FileLocation.HTTP, "http://domain/some-file"),
    (FileLocation.HTTPS, "https://domain/some-file"),
]
sample_filepaths = [items[1] for items in sample_filepaths_per_location]
sample_filepaths_ids = [items[0].value for items in sample_filepaths_per_location]


def describe_get_location_type():
    """test get_location_type()"""

    @pytest.mark.parametrize(
        "expected_location,filepath",
        sample_filepaths_per_location,
        ids=sample_filepaths_ids,
    )  # skipcq: PTC-W0065
    def with_supported_location(expected_location, filepath):  # skipcq: PTC-W0065
        """With all the supported locations"""
        location = Location.get_location_type(filepath)
        assert location == expected_location


def describe_get_transport_params():
    """test get_transport_params() method"""

    def with_api():  # skipcq: PYL-W0612, PTC-W0065
        """with API endpoint"""
        path = "http://domain/file"
        location = location_factory(path)
        credentials = location.get_transport_params()
        assert credentials is None

        path = "https://domain/file"
        location = location_factory(path)
        credentials = location.get_transport_params()
        assert credentials is None


def describe_get_paths():
    """test get_paths() method"""

    def with_api():  # skipcq: PYL-W0612, PTC-W0065
        """with API endpoint"""
        path = "http://domain/some-file.txt"
        location = location_factory(path)
        assert location.get_paths() == [path]

        path = "https://domain/some-file.txt"
        location = location_factory(path)
        assert location.get_paths() == [path]
