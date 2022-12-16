from astro.files.locations.azure.wasb import WASBLocation


def test_wasb_location_openlineage_dataset_namespace():
    location = WASBLocation(path="wasbs://blobaccount@owshqblobstg.blob.core.windows.net/container")
    assert location.openlineage_dataset_namespace == "wasbs://blobaccount@owshqblobstg.blob.core.windows.net"


def test_wasb_location_openlineage_dataset_name():
    location = WASBLocation(path="wasbs://blobaccount@owshqblobstg.blob.core.windows.net/container")
    assert location.openlineage_dataset_name == "/container"
