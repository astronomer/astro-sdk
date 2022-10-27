import os
from unittest.mock import patch

import pytest
from botocore.client import BaseClient

from astro.files.locations import create_file_location
from astro.files.locations.amazon.s3 import S3Location


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
    assert sorted(location.paths) == sorted(["s3://tmp/house1.csv", "s3://tmp/house2.csv"])


@pytest.mark.integration
def test_size():
    """Test get_size() of for S3 file."""
    location = S3Location(path="s3://astro-sdk/imdb.csv", conn_id="aws_conn")
    assert location.size > 0


@patch.dict(
    os.environ,
    {"AWS_ACCESS_KEY_ID": "abcd", "AWS_SECRET_ACCESS_KEY": "@#$%@$#ASDH@Ksd23%SD546"},
)
def test_parse_s3_env_var():
    key, secret = S3Location._parse_s3_env_var()
    assert key == "abcd"
    assert secret == "@#$%@$#ASDH@Ksd23%SD546"


def test_get_connection_extras():
    """Test get_connection_extras() return correct extras"""
    location = S3Location(path="s3://astro-sdk/imdb.csv", conn_id="minio_conn")
    extras = location.get_connection_extras()
    assert extras == {
        "aws_access_key_id": "ROOTNAME",
        "aws_secret_access_key": "CHANGEME123",
        "endpoint_url": "http://127.0.0.1:9000",
    }


def test_transport_params_is_calling_get_connection_extras():
    """
    Test that get_connection_extras() is called when getting transport_params
    """
    with patch("astro.files.locations.amazon.s3.S3Location.get_connection_extras") as get_connection_extras:
        get_connection_extras.return_value = {}
        location = S3Location(path="s3://astro-sdk/imdb.csv", conn_id="minio_conn")
        _ = location.transport_params
        get_connection_extras.assert_called()


def test_transport_params_calls_with_correct_kwargs():
    """
    Test that we pass correct arguments to session.client()
    """

    class Dummy:
        @staticmethod
        def client(**kwargs):
            assert kwargs == {
                "aws_access_key_id": "ROOTNAME",
                "aws_secret_access_key": "CHANGEME123",
                "endpoint_url": "http://127.0.0.1:9000",
                "service_name": "s3",
            }

    with patch("airflow.providers.amazon.aws.hooks.s3.S3Hook.get_session") as session:
        session.return_value = Dummy()
        location = S3Location(path="s3://astro-sdk/imdb.csv", conn_id="minio_conn")
        _ = location.transport_params
