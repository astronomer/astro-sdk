import pathlib
from unittest import mock

import pytest
from google.cloud.bigquery_datatransfer_v1.types import (
    StartManualTransferRunsResponse,
    TransferConfig,
    TransferRun,
)

from astro import settings
from astro.databases.google.bigquery import BigqueryDatabase, S3ToBigqueryDataTransfer
from astro.exceptions import DatabaseCustomError
from astro.files import File
from astro.table import TEMP_PREFIX, Metadata, Table

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


@pytest.mark.parametrize(
    "source_table,input_table,returned_table,source_location",
    [
        # Table Metadata is empty and it is copied from Default Metadata
        (
            None,
            Table(name="s1"),
            Table(name="s1", metadata=Metadata(schema=settings.BIGQUERY_SCHEMA, database="test_project_id")),
            settings.DEFAULT_BIGQUERY_SCHEMA_LOCATION,
        ),
        # Table Metadata just contains database/project_id and only schema is copied from Default Metadata
        (
            None,
            Table(name="s1", metadata=Metadata(database="test_project_id_2")),
            Table(
                name="s1", metadata=Metadata(schema=settings.BIGQUERY_SCHEMA, database="test_project_id_2")
            ),
            settings.DEFAULT_BIGQUERY_SCHEMA_LOCATION,
        ),
        # Table Metadata contains both schema & database/project_id and the table remains unchanged
        (
            None,
            Table(name="s1", metadata=Metadata(schema="test_schema", database="test_project_id_2")),
            Table(name="s1", metadata=Metadata(schema="test_schema", database="test_project_id_2")),
            settings.DEFAULT_BIGQUERY_SCHEMA_LOCATION,
        ),
        # Table is temp and its Metadata is empty but the source_table is not None but its metadata is empty
        # so the metadata is copied from the default metadata
        (
            Table(name="t1"),
            Table(name=f"{TEMP_PREFIX}_xyz"),
            Table(
                name=f"{TEMP_PREFIX}_xyz",
                metadata=Metadata(schema=settings.BIGQUERY_SCHEMA, database="test_project_id"),
            ),
            settings.DEFAULT_BIGQUERY_SCHEMA_LOCATION,
        ),
        (
            Table(name="t1", metadata=Metadata(schema="test_schema")),
            Table(name=f"{TEMP_PREFIX}_xyz"),
            Table(
                name=f"{TEMP_PREFIX}_xyz",
                metadata=Metadata(schema=settings.BIGQUERY_SCHEMA, database="test_project_id"),
            ),
            settings.DEFAULT_BIGQUERY_SCHEMA_LOCATION,
        ),
        (
            Table(name="t1", metadata=Metadata(schema="test_schema", database="test_project_id2")),
            Table(name=f"{TEMP_PREFIX}_xyz"),
            Table(
                name=f"{TEMP_PREFIX}_xyz",
                metadata=Metadata(schema=settings.BIGQUERY_SCHEMA, database="test_project_id2"),
            ),
            settings.DEFAULT_BIGQUERY_SCHEMA_LOCATION,
        ),
        (
            Table(name="t1", metadata=Metadata(schema="schema_in_eu_west2", database="test_project_id2")),
            Table(name=f"{TEMP_PREFIX}_xyz"),
            Table(
                name=f"{TEMP_PREFIX}_xyz",
                metadata=Metadata(
                    schema=f"{settings.BIGQUERY_SCHEMA}__europe_west2", database="test_project_id2"
                ),
            ),
            "europe-west2",
        ),
        (
            Table(name="t1", metadata=Metadata(schema="schema_in_eu_west2")),
            Table(name=f"{TEMP_PREFIX}_xyz"),
            Table(
                name=f"{TEMP_PREFIX}_xyz",
                metadata=Metadata(
                    schema=f"{settings.BIGQUERY_SCHEMA}__europe_west2", database="test_project_id"
                ),
            ),
            "europe-west2",
        ),
    ],
)
@mock.patch("astro.databases.google.bigquery.BigQueryHook", autospec=True)
def test_populate_table_metadata(mock_bq_hook, source_table, input_table, returned_table, source_location):
    bq_hook = mock.MagicMock(project_id="test_project_id")
    mock_bq_hook.return_value = bq_hook

    def mock_get_dataset(dataset_id):
        if dataset_id == settings.BIGQUERY_SCHEMA:
            return mock.MagicMock(location=settings.DEFAULT_BIGQUERY_SCHEMA_LOCATION)
        return mock.MagicMock(location=source_location)

    mock_bq_hook.return_value.get_dataset.side_effect = mock_get_dataset

    db = BigqueryDatabase(table=source_table, conn_id="test_conn")
    assert db.populate_table_metadata(input_table) == returned_table


@mock.patch("astro.databases.google.bigquery.BigqueryDatabase.hook")
def test_get_project_id_raise_exception(mock_hook):
    """
    Test loading on files to bigquery natively for fallback without fallback
    gracefully for wrong file location.
    """

    class CustomAttributeError:
        def __str__(self):
            raise AttributeError

    database = BigqueryDatabase()
    mock_hook.project_id = CustomAttributeError()

    with pytest.raises(DatabaseCustomError):
        database.get_project_id(target_table=Table())
