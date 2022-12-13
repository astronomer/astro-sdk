"""Tests specific to the Sqlite Database implementation."""
import pathlib

from google.cloud.bigquery_datatransfer_v1.types import (
    StartManualTransferRunsResponse,
    TransferConfig,
    TransferRun,
)

from astro.databases.google.bigquery import BigqueryDatabase, S3ToBigqueryDataTransfer
from astro.files import File

DEFAULT_CONN_ID = "google_cloud_default"
CUSTOM_CONN_ID = "gcp_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent


def test_is_native_autodetect_schema_available():
    """
    Test if native autodetect schema is available for S3 and GCS.
    """
    db = BigqueryDatabase(conn_id="fake_conn_id")
    assert db.is_native_autodetect_schema_available(file=File(path="s3://bucket/key.csv")) is False

    assert db.is_native_autodetect_schema_available(file=File(path="gs://bucket/key.csv")) is True


def test_get_transfer_config_id():
    config = TransferConfig()
    config.name = "projects/103191871648/locations/us/transferConfigs/6302bf19-0000-26cf-a568-94eb2c0a61ee"
    assert S3ToBigqueryDataTransfer.get_transfer_config_id(config) == "6302bf19-0000-26cf-a568-94eb2c0a61ee"


def test_get_run_id():
    config = StartManualTransferRunsResponse()
    run = TransferRun()
    run.name = (
        "projects/103191871648/locations/us/transferConfigs/"
        "62d38894-0000-239c-a4d8-089e08325b54/runs/62d6a4df-0000-2fad-8752-d4f547e68ef4"
    )
    config.runs.append(run)
    assert S3ToBigqueryDataTransfer.get_run_id(config) == "62d6a4df-0000-2fad-8752-d4f547e68ef4"
