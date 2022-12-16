from astro.files.locations.azure.wasb import WASBLocation


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
