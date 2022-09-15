"""Tests specific to the Sqlite Database implementation."""
import os
import pathlib
from unittest import mock
from urllib.parse import urlparse

import pandas as pd
import pytest
import sqlalchemy
from astro.constants import Database
from astro.databases import create_database
from astro.databases.google.bigquery import BigqueryDatabase, S3ToBigqueryDataTransfer
from astro.exceptions import DatabaseCustomError, NonExistentTableException
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table
from astro.utils.load import copy_remote_file_to_local
from google.cloud.bigquery_datatransfer_v1.types import (
    StartManualTransferRunsResponse,
    TransferConfig,
    TransferRun,
)
from tests.sql.operators import utils as test_utils

DEFAULT_CONN_ID = "google_cloud_default"
CUSTOM_CONN_ID = "gcp_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent


TEST_TABLE = Table()


# To Do: How are the default connection created for providers bigquery.
@pytest.mark.parametrize("conn_id", SUPPORTED_CONN_IDS)
def test_create_database(conn_id):
    """Test creation of database"""
    database = create_database(conn_id)
    assert isinstance(database, BigqueryDatabase)


@pytest.mark.parametrize(
    "conn_id,expected_uri",
    [
        (DEFAULT_CONN_ID, "bigquery://astronomer-dag-authoring"),
        (CUSTOM_CONN_ID, "bigquery://astronomer-dag-authoring"),
    ],
    ids=SUPPORTED_CONN_IDS,
)
@mock.patch(
    "astro.databases.google.bigquery.BigQueryHook.provide_gcp_credential_file_as_context"
)
def test_bigquery_sqlalchemy_engine(mock_credentials, conn_id, expected_uri):
    """Test getting a bigquery based sqla engine."""
    database = BigqueryDatabase(conn_id)
    engine = database.sqlalchemy_engine
    assert isinstance(engine, sqlalchemy.engine.base.Engine)
    url = urlparse(str(engine.url))
    assert url.geturl() == expected_uri
    mock_credentials.assert_called_once_with()


@pytest.mark.integration
def test_bigquery_run_sql():
    """Test run_sql against bigquery database"""
    statement = "SELECT 1 + 1;"
    database = BigqueryDatabase(conn_id=DEFAULT_CONN_ID)
    response = database.run_sql(statement)
    assert response.first()[0] == 2


@pytest.mark.integration
def test_table_exists_raises_exception():
    """Test if table exists in bigquery database"""
    database = BigqueryDatabase(conn_id=DEFAULT_CONN_ID)
    table = Table(name="inexistent-table", metadata=Metadata(schema=SCHEMA))
    assert not database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(
                metadata=Metadata(schema=SCHEMA),
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column(
                        "name", sqlalchemy.String(60), nullable=False, key="name"
                    ),
                ],
            ),
        }
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_bigquery_create_table_with_columns(database_table_fixture):
    """Test table creation with columns data"""
    database, table = database_table_fixture

    # Looking for specific columns in INFORMATION_SCHEMA.COLUMNS as Bigquery can add/remove columns in the table.
    statement = (
        f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE "
        f"FROM {table.metadata.schema}.INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table.name}'"
    )
    response = database.run_sql(statement)
    assert response.first() is None

    database.create_table(table)
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (
        "astronomer-dag-authoring",
        f"{table.metadata.schema}",
        f"{table.name}",
        "id",
        "INT64",
    )

    assert rows[1] == (
        "astronomer-dag-authoring",
        f"{table.metadata.schema}",
        f"{table.name}",
        "name",
        "STRING(60)",
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(
                metadata=Metadata(schema=SCHEMA),
            ),
        }
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_bigquery_create_table_using_native_schema_autodetection(
    database_table_fixture,
):
    """Test table creation using native schema autodetection"""
    database, table = database_table_fixture

    # Looking for specific columns in INFORMATION_SCHEMA.COLUMNS as Bigquery can add/remove columns in the table.
    statement = (
        f"SELECT TABLE_CATALOG, TABLE_SCHEMA, TABLE_NAME, COLUMN_NAME, DATA_TYPE "
        f"FROM {table.metadata.schema}.INFORMATION_SCHEMA.COLUMNS WHERE table_name='{table.name}'"
    )
    response = database.run_sql(statement)
    assert response.first() is None

    file = File("gs://astro-sdk/workspace/test-sample-njson.ndjson", conn_id="gcp_conn")
    database.create_table(table, file)
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    assert rows == [
        (
            "astronomer-dag-authoring",
            table.metadata.schema,
            table.name,
            "result",
            (
                "STRUCT<pageData STRUCT<timestamp INT64, statusCode INT64>,"
                " sequenceNumber INT64, timestamp INT64, extractorData STRUCT<data"
                " ARRAY<STRUCT<`group` ARRAY<STRUCT<Business ARRAY<STRUCT<text STRING,"
                " href STRING>>>>>>, url STRING>>"
            ),
        ),
        (
            "astronomer-dag-authoring",
            table.metadata.schema,
            table.name,
            "url",
            "STRING",
        ),
    ]
    statement = f"SELECT COUNT(*) FROM {database.get_table_qualified_name(table)}"
    count = database.run_sql(statement).scalar()
    assert count == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_load_pandas_dataframe_to_table(database_table_fixture):
    """Test load_pandas_dataframe_to_table against bigquery"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT * FROM {database.get_table_qualified_name(table)};"
    response = database.run_sql(statement)

    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)


def test_is_native_autodetect_schema_available():
    """
    Test if native autodetect schema is available for S3 and GCS.
    """
    db = BigqueryDatabase(conn_id="fake_conn_id")
    assert (
        db.is_native_autodetect_schema_available(file=File(path="s3://bucket/key.csv"))
        is False
    )

    assert (
        db.is_native_autodetect_schema_available(file=File(path="gs://bucket/key.csv"))
        is True
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_load_file_to_table(database_table_fixture):
    """Test loading on files to bigquery database"""
    database, target_table = database_table_fixture
    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    database.load_file_to_table(File(filepath), target_table, {})

    df = database.hook.get_pandas_df(
        f"SELECT * FROM {database.get_table_qualified_name(target_table)}"
    )
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    test_utils.assert_dataframes_are_equal(df, expected)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_load_file_to_table_natively_for_not_optimised_path(database_table_fixture):
    """Test loading on files to bigquery natively for non optimized path."""
    database, target_table = database_table_fixture
    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    response = database.load_file_to_table_natively(File(filepath), target_table)
    assert response is None


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
@mock.patch(
    "astro.databases.google.bigquery.BigqueryDatabase.load_file_to_table_natively"
)
def test_load_file_to_table_natively_for_fallback(
    mock_load_file, database_table_fixture
):
    """Test loading on files to bigquery natively for fallback."""
    mock_load_file.side_effect = DatabaseCustomError
    database, target_table = database_table_fixture
    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    response = database.load_file_to_table_natively_with_fallback(
        File(filepath), target_table
    )
    assert response is None


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_load_file_to_table_natively_for_fallback_wrong_file_location_with_enable_native_fallback(
    database_table_fixture,
):
    """
    Test loading on files to bigquery natively for fallback without fallback
    gracefully for wrong file location.
    """
    database, target_table = database_table_fixture
    filepath = "https://www.data.com/data/sample.json"

    with pytest.raises(DatabaseCustomError):
        database.load_file_to_table_natively_with_fallback(
            source_file=File(filepath),
            target_table=target_table,
            enable_native_fallback=False,
        )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
@mock.patch("astro.databases.google.bigquery.BigqueryDatabase.hook")
def test_get_project_id_raise_exception(
    mock_hook,
    database_table_fixture,
):
    """
    Test loading on files to bigquery natively for fallback without fallback
    gracefully for wrong file location.
    """

    class CustomAttibuteError:
        def __str__(self):
            raise AttributeError

    mock_hook.project_id = CustomAttibuteError()
    database, target_table = database_table_fixture

    with pytest.raises(DatabaseCustomError):
        database.get_project_id(target_table=target_table)


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_export_table_to_file_file_already_exists_raises_exception(
    database_table_fixture,
):
    """
    Test export_table_to_file_file() where the end file already exists, should result in exception
    when the override option is False
    """
    database, source_table = database_table_fixture
    filepath = pathlib.Path(CWD.parent, "data/sample.csv")
    with pytest.raises(FileExistsError) as exception_info:
        database.export_table_to_file(source_table, File(str(filepath)))
    err_msg = exception_info.value.args[0]
    assert err_msg.endswith(f"The file {filepath} already exists.")


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_export_table_to_file_overrides_existing_file(database_table_fixture):
    """
    Test export_table_to_file_file() where the end file already exists,
    should result in overriding the existing file
    """
    database, populated_table = database_table_fixture

    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
    database.export_table_to_file(populated_table, File(filepath), if_exists="replace")

    df = test_utils.load_to_dataframe(filepath, "csv")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    assert df.rename(columns=str.lower).equals(expected)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.BIGQUERY, "table": Table(metadata=Metadata(schema=SCHEMA))}],
    indirect=True,
    ids=["bigquery"],
)
def test_export_table_to_pandas_dataframe_non_existent_table_raises_exception(
    database_table_fixture,
):
    """Test export_table_to_file_file() where the table don't exist, should result in exception"""
    database, non_existent_table = database_table_fixture

    with pytest.raises(NonExistentTableException) as exc_info:
        database.export_table_to_pandas_dataframe(non_existent_table)
    error_message = exc_info.value.args[0]
    assert error_message.startswith("The table")
    assert error_message.endswith("does not exist")


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["bigquery"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google", "file_create": False}],
    indirect=True,
    ids=["google"],
)
def test_export_table_to_file_in_the_cloud(
    database_table_fixture, remote_files_fixture
):
    """Test export_table_to_file_file() where end file location is in cloud object stores"""
    object_path = remote_files_fixture[0]
    database, populated_table = database_table_fixture

    database.export_table_to_file(
        populated_table,
        File(object_path),
        if_exists="replace",
    )

    filepath = copy_remote_file_to_local(object_path)
    df = pd.read_csv(filepath)
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    test_utils.assert_dataframes_are_equal(df, expected)
    os.remove(filepath)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["bigquery"],
)
def test_create_table_from_select_statement(database_table_fixture):
    """Test table creation via select statement"""
    database, original_table = database_table_fixture

    statement = "SELECT * FROM {} WHERE id = 1;".format(
        database.get_table_qualified_name(original_table)
    )
    target_table = Table(metadata=Metadata(schema=SCHEMA))
    database.create_table_from_select_statement(statement, target_table)

    df = database.hook.get_pandas_df(
        f"SELECT * FROM {database.get_table_qualified_name(target_table)}"
    )
    assert len(df) == 1
    expected = pd.DataFrame([{"id": 1, "name": "First"}])
    test_utils.assert_dataframes_are_equal(df, expected)
    database.drop_table(target_table)


def test_get_transfer_config_id():
    config = TransferConfig()
    config.name = "projects/103191871648/locations/us/transferConfigs/6302bf19-0000-26cf-a568-94eb2c0a61ee"
    assert (
        S3ToBigqueryDataTransfer.get_transfer_config_id(config)
        == "6302bf19-0000-26cf-a568-94eb2c0a61ee"
    )


def test_get_run_id():
    config = StartManualTransferRunsResponse()
    run = TransferRun()
    run.name = (
        "projects/103191871648/locations/us/transferConfigs/"
        "62d38894-0000-239c-a4d8-089e08325b54/runs/62d6a4df-0000-2fad-8752-d4f547e68ef4"
    )
    config.runs.append(run)
    assert (
        S3ToBigqueryDataTransfer.get_run_id(config)
        == "62d6a4df-0000-2fad-8752-d4f547e68ef4"
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.BIGQUERY,
            "table": Table(metadata=Metadata(schema=SCHEMA)),
        },
    ],
    indirect=True,
    ids=["bigquery"],
)
@mock.patch("astro.databases.google.bigquery.pd.DataFrame")
def test_load_pandas_dataframe_to_table_with_service_account(
    mock_df, database_table_fixture
):
    """Test loading a pandas dataframe to a table with service account authentication."""
    database, target_table = database_table_fixture
    # Skip running _get_credentials. We assume we always will get a Credentials object back.
    database.hook._get_credentials = mock.Mock()

    database.load_pandas_dataframe_to_table(mock_df, target_table)

    _, kwargs = mock_df.to_gbq.call_args
    # Check that we are using service account authentication i.e. by passing not None.
    assert kwargs["credentials"] is not None
