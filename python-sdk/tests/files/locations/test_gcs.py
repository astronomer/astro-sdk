from unittest.mock import patch

from astro.files.locations import create_file_location


@patch(
    "airflow.providers.google.cloud.hooks.gcs.GCSHook.list",
    return_value=["house1.csv", "house2.csv"],
)
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    location = create_file_location("gs://tmp/house")
    assert sorted(location.paths) == sorted(["gs://tmp/house1.csv", "gs://tmp/house2.csv"])
