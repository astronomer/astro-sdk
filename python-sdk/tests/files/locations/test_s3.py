import os
from unittest.mock import patch

from astro.files.locations.amazon.s3 import S3Location
from botocore.client import BaseClient

from astro.files.locations import create_file_location


def test_get_transport_params_with_s3():  # skipcq: PYL-W0612
    """test get_transport_params() method with S3 filepath"""
    path = "s3://bucket/some-file"
    location = create_file_location(path)
    credentials = location.transport_params
    assert isinstance(credentials["client"], BaseClient)


@patch(
    "airflow.providers.amazon.aws.hooks.s3.S3Hook.list_keys",
    return_value=["house1.csv", "house2.csv"],
)
def test_remote_object_store_prefix(remote_file):
    """with remote filepath having prefix"""
    location = create_file_location("s3://tmp/house")
    assert sorted(location.paths) == sorted(
        ["s3://tmp/house1.csv", "s3://tmp/house2.csv"]
    )


def test_size():
    """Test get_size() of for local file."""
    location = create_file_location("s3://tmp/house2.csv")
    assert location.size == -1


@patch.dict(
    os.environ,
    {"AWS_ACCESS_KEY_ID": "abcd", "AWS_SECRET_ACCESS_KEY": "@#$%@$#ASDH@Ksd23%SD546"},
)
def test_parse_s3_env_var():
    key, secret = S3Location._parse_s3_env_var()
    assert key == "abcd"
    assert secret == "@#$%@$#ASDH@Ksd23%SD546"
