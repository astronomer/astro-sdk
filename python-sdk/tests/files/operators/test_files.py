import pathlib
from unittest.mock import patch

from airflow.models.connection import Connection
from astro.files.operators.files import ListFileOperator


def test_get_file_list_execute_local():
    """Assert that when file object location point to local then get_file_list using local location interface"""
    CWD = pathlib.Path(__file__).parent
    LOCAL_FILEPATH = f"{CWD}/../../example_dags/data/"

    op = ListFileOperator(task_id="task_id", conn_id="conn_id", path=LOCAL_FILEPATH)
    actual = op.execute(None)
    assert actual == [str(file) for file in pathlib.Path(LOCAL_FILEPATH).rglob("*")]


@patch("astro.files.locations.google.gcs.GCSLocation.hook")
def test_get_file_list_execute_gcs(hook):
    """Assert that when file object location point to GCS then get_file_list using GCSHook"""
    hook.return_value = Connection(conn_id="conn", conn_type="google_cloud_platform")
    op = ListFileOperator(
        task_id="task_id",
        conn_id="conn",
        path="gs://bucket/some-file",
    )
    op.execute(None)
    hook.list.assert_called_once_with(bucket_name="bucket", prefix="some-file")


@patch("astro.files.locations.amazon.s3.S3Location.hook")
def test_get_file_list_s3(hook):
    """Assert that when file object location point to s3 then get_file_list using S3Hook"""
    hook.return_value = Connection(conn_id="conn", conn_type="s3")
    op = ListFileOperator(
        task_id="task_id",
        conn_id="conn",
        path="s3://bucket/some-file",
    )
    op.execute(None)
    hook.list_keys.assert_called_once_with(bucket_name="bucket", prefix="some-file")
