from unittest.mock import patch

from botocore.client import BaseClient

from astro.files.locations import location_factory


def test_get_transport_params_with_s3():  # skipcq: PYL-W0612
    """test get_transport_params() method with S3 filepath"""
    path = "s3://bucket/some-file"
    location = location_factory(path)
    credentials = location.get_transport_params()
    assert isinstance(credentials["client"], BaseClient)


@patch(
    "airflow.providers.amazon.aws.hooks.s3.S3Hook.list_keys",
    return_value=["house1.csv", "house2.csv"],
)
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    location = location_factory("s3://tmp/house")
    assert sorted(location.get_paths()) == sorted(
        ["s3://tmp/house1.csv", "s3://tmp/house2.csv"]
    )
