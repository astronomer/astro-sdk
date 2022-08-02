import pathlib
from unittest.mock import patch

from airflow.models.connection import Connection

from astro.dtt.files import get_file_list

CWD = pathlib.Path(__file__).parent
LOCAL_FILEPATH = f"{CWD}/../../example_dags/data/"


def test_get_file_list_local():
    """Assert that when file object location point to local then get_file_list using local location interface"""
    paths = get_file_list(LOCAL_FILEPATH, "conn")
    assert paths == [str(file) for file in pathlib.Path(LOCAL_FILEPATH).rglob("*")]


@patch("astro.files.locations.google.gcs.GCSLocation.hook")
def test_get_file_list_gcs(hook):
    """Assert that when file object location point to GCS then get_file_list using GCSHook"""
    hook.return_value = Connection(
        conn_id="conn",
        conn_type="google_cloud_platform"
    )
    get_file_list("gs://bucket/some-file", "conn")
    hook.list.assert_called_once()


@patch("astro.files.locations.amazon.s3.S3Location.hook")
def test_get_file_list_s3(hook):
    """Assert that when file object location point to s3 then get_file_list using S3Hook"""
    hook.return_value = Connection(
        conn_id="conn",
        conn_type="s3"
    )
    get_file_list("s3://bucket/some-file", "conn")
    hook.list_keys.assert_called_once()
