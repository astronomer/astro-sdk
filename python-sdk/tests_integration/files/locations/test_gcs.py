import pytest
from google.cloud.storage import Client

from astro.files.locations import create_file_location


@pytest.mark.integration
def test_size():
    """Test get_size() of for GCS file."""
    path = "gs://astro-sdk/workspace/sample_pattern.csv"
    location = create_file_location(path)
    assert location.size > 0


@pytest.mark.integration
def test_get_transport_params_for_gcs():  # skipcq: PYL-W0612, PTC-W0065
    """test get_transport_params() method which should return gcs client"""
    path = "gs://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert isinstance(credentials["client"], Client)
