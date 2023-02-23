from unittest.mock import patch

import pytest
from azure.storage.blob import BlobServiceClient

from astro.files.locations import create_file_location
from astro.files.locations.azure.wasb import WASBLocation
from astro.options import WASBLocationLoadOptions


def test_get_transport_params_with_wasb():
    """test get_transport_params() method with WASB filepath"""
    path = "wasbs://container@storageaccount.blob.core.windows.net"
    location = create_file_location(path)
    credentials = location.transport_params
    assert isinstance(credentials["client"], BlobServiceClient)


@patch(
    "airflow.providers.microsoft.azure.hooks.wasb.WasbHook.get_blobs_list",
    return_value=["house1.csv", "house2.csv"],
)
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    location = create_file_location("wasb://tmp/house")
    assert isinstance(location, WASBLocation)
    assert sorted(location.paths) == sorted(["wasb://tmp/house1.csv", "wasb://tmp/house2.csv"])


def test_wasb_location_openlineage_dataset_namespace():
    location = WASBLocation(path="wasbs://blobaccount@owshqblobstg.blob.core.windows.net/container")
    assert location.openlineage_dataset_namespace == "wasbs://blobaccount@owshqblobstg.blob.core.windows.net"


def test_wasb_location_openlineage_dataset_name():
    location = WASBLocation(path="wasbs://blobaccount@owshqblobstg.blob.core.windows.net/container")
    assert location.openlineage_dataset_name == "/container"


def test_wasb_smartopen_uri_wasbs_scheme():
    location = WASBLocation(path="wasbs://somepath")
    assert location.smartopen_uri == "azure://somepath"


def test_wasb_smartopen_uri_azure_scheme():
    location = WASBLocation(path="azure://somepath")
    assert location.smartopen_uri == "azure://somepath"


def test_snowflake_stage_path_raise_exception():
    """
    Test snowflake_stage_path raise exception when 'storage_account' is missing.
    """
    location = WASBLocation(path="azure://somepath")
    error_message = f"Required param missing 'storage_account', pass {location.LOAD_OPTIONS_CLASS_NAME[0]}"
    "(storage_account=<account_name>) to load_options"
    with pytest.raises(ValueError, match=error_message):
        location.snowflake_stage_path


def test_snowflake_stage_path_new_path():
    """
    Test snowflake_stage_path to return expected path
    """
    location = WASBLocation(
        path="wasb://some_container/test.csv",
        load_options=WASBLocationLoadOptions(storage_account="storage_account_name"),
    )
    new_path = location.snowflake_stage_path
    assert new_path == "azure://storage_account_name.blob.core.windows.net/some_container/"
