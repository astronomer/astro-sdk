from unittest.mock import patch

from google.cloud.storage import Client

from astro.files.locations import location_factory


def test_get_transport_params_for_gcs():  # skipcq: PYL-W0612, PTC-W0065
    """test get_transport_params() method which should return gcs client"""
    path = "gs://bucket/some-file"
    location = location_factory(path)
    credentials = location.get_transport_params()
    assert isinstance(credentials["client"], Client)


@patch(
    "airflow.providers.google.cloud.hooks.gcs.GCSHook.list",
    return_value=["house1.csv", "house2.csv"],
)
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    location = location_factory("gs://tmp/house")
    assert sorted(location.get_paths()) == sorted(
        ["gs://tmp/house1.csv", "gs://tmp/house2.csv"]
    )
