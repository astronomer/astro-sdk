import pytest

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation

sample_filepaths_per_location = [
    (FileLocation.HTTP, "http://tenant-prefix.sharepoint.com/some-file"),
    (FileLocation.HTTPS, "https://tenant-prefix.sharepoint.com/some-file"),
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
