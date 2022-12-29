from pathlib import Path

import pytest

from astro.files.locations import create_file_location
from astro.files.locations.azure.wasb import WASBLocation


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "azure", "file_count": 2, "conn_id": "wasb_default_conn"}],
    indirect=True,
    ids=["azure_wasb"],
)
def test_remote_object_store_connection(remote_files_fixture):
    """integration test to confirm if location is able to access blob storage"""
    location = create_file_location("wasb://astro-sdk/", conn_id="wasb_default_conn")
    expected_blobs_list = [Path(item).name for item in remote_files_fixture]
    actual_blobs_list = location.hook.get_blobs_list(container_name="astro-sdk", prefix="test/")
    actual_blobs_list = [Path(item).name for item in actual_blobs_list]
    assert set(expected_blobs_list).issubset(set(actual_blobs_list))


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "azure", "file_count": 1}],
    indirect=True,
    ids=["azure_wasb"],
)
def test_size(remote_files_fixture):
    """Test get_size() of for Blob Storage file."""
    location = WASBLocation(path=remote_files_fixture[0], conn_id="wasb_default_conn")
    assert location.size == 65
