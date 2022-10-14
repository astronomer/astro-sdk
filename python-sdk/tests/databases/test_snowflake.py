"""Tests specific to the Sqlite Database implementation."""
import os
import pathlib
from unittest import mock
from unittest.mock import ANY, patch

import pandas as pd
import pytest
import sqlalchemy
from sqlalchemy.exc import ProgrammingError

from astro.constants import Database, FileLocation, FileType
from astro.databases import create_database
from astro.databases.snowflake import SnowflakeDatabase, SnowflakeFileFormat, SnowflakeStage
from astro.exceptions import DatabaseCustomError, NonExistentTableException
from astro.files import File
from astro.settings import (
    SNOWFLAKE_SCHEMA,
    SNOWFLAKE_STORAGE_INTEGRATION_AMAZON,
    SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE,
)
from astro.table import Metadata, Table
from astro.utils.load import copy_remote_file_to_local
from tests.sql.operators import utils as test_utils

DEFAULT_CONN_ID = "snowflake_default"
CUSTOM_CONN_ID = "snowflake_conn"
SUPPORTED_CONN_IDS = [CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent

TEST_TABLE = Table()


SNOWFLAKE_STORAGE_INTEGRATION_AMAZON = SNOWFLAKE_STORAGE_INTEGRATION_AMAZON or "aws_int_python_sdk"
SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE = SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE or "gcs_int_python_sdk"


@pytest.mark.parametrize("conn_id", SUPPORTED_CONN_IDS)
def test_create_database(conn_id):
    """Test creation of database"""
    database = create_database(conn_id)
    assert isinstance(database, SnowflakeDatabase)


@pytest.mark.integration
def test_snowflake_run_sql():
    """Test run_sql against snowflake database"""
    statement = "SELECT 1 + 1;"
    database = SnowflakeDatabase(conn_id=CUSTOM_CONN_ID)
    response = database.run_sql(statement)
    assert response.first()[0] == 2


@pytest.mark.integration
def test_table_exists_raises_exception():
    """Test if table exists in snowflake database"""
    database = SnowflakeDatabase(conn_id=CUSTOM_CONN_ID)
    table = Table(name="inexistent-table", metadata=Metadata(schema=SNOWFLAKE_SCHEMA))
    assert not database.table_exists(table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(
                metadata=Metadata(schema=SNOWFLAKE_SCHEMA),
                columns=[
                    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
                    sqlalchemy.Column("name", sqlalchemy.String(60), nullable=False, key="name"),
                ],
            ),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_snowflake_create_table_with_columns(database_table_fixture):
    """Test table creation with columns data"""
    database, table = database_table_fixture

    statement = f"DESC TABLE {database.get_table_qualified_name(table)}"
    with pytest.raises(ProgrammingError) as e:
        database.run_sql(statement)
    assert e.match("does not exist or not authorized")

    database.create_table(table)
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (
        "ID",
        "NUMBER(38,0)",
        "COLUMN",
        "N",
        "IDENTITY START 1 INCREMENT 1",
        "Y",
        "N",
        None,
        None,
        None,
        None,
    )
    assert rows[1] == (
        "NAME",
        "VARCHAR(60)",
        "COLUMN",
        "N",
        None,
        "N",
        "N",
        None,
        None,
        None,
        None,
    )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(
                metadata=Metadata(schema=SNOWFLAKE_SCHEMA),
            ),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_snowflake_create_table_using_native_schema_autodetection(
    database_table_fixture,
):
    """Test table creation using native schema autodetection"""
    database, table = database_table_fixture

    statement = f"DESC TABLE {database.get_table_qualified_name(table)}"
    with pytest.raises(ProgrammingError) as e:
        database.run_sql(statement)
    assert e.match("does not exist or not authorized")

    file = File("s3://astro-sdk/sample.parquet", conn_id="aws_conn")
    database.create_table(table, file)
    response = database.run_sql(statement)
    rows = response.fetchall()
    assert len(rows) == 2
    statement = f"SELECT COUNT(*) FROM {database.get_table_qualified_name(table)}"
    count = database.run_sql(statement).scalar()
    assert count == 0


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_load_pandas_dataframe_to_table(database_table_fixture):
    """Test load_pandas_dataframe_to_table against snowflake"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})
    database.load_pandas_dataframe_to_table(pandas_dataframe, table)

    statement = f"SELECT * FROM {database.get_table_qualified_name(table)}"
    response = database.run_sql(statement)

    rows = response.fetchall()
    assert len(rows) == 2
    assert rows[0] == (1,)
    assert rows[1] == (2,)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_if_exist_param_of__load_pandas_dataframe_to_table(database_table_fixture):
    """Test if_exist parameter on load_pandas_dataframe_to_table against snowflake"""
    database, table = database_table_fixture

    pandas_dataframe = pd.DataFrame(data={"id": [1, 2]})

    with mock.patch("snowflake.connector.pandas_tools.write_pandas") as method:
        database.load_pandas_dataframe_to_table(pandas_dataframe, table, if_exists="replace")
        method.assert_called_with(
            conn=ANY,
            df=ANY,
            table_name=ANY,
            schema=ANY,
            database=ANY,
            chunk_size=ANY,
            quote_identifiers=True,
            auto_create_table=True,
        )

    with mock.patch("astro.databases.base.BaseDatabase.table_exists") as table_exists:
        table_exists.return_value = True
        with mock.patch("snowflake.connector.pandas_tools.write_pandas") as method:
            database.load_pandas_dataframe_to_table(pandas_dataframe, table, if_exists="append")
            method.assert_called_with(
                conn=ANY,
                df=ANY,
                table_name=ANY,
                schema=ANY,
                database=ANY,
                chunk_size=ANY,
                quote_identifiers=True,
                auto_create_table=False,
            )


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_load_file_to_table(database_table_fixture):
    """Test loading on files to snowflake database"""
    database, target_table = database_table_fixture
    filepath = str(pathlib.Path(CWD.parent, "data/sub_folder/"))
    database.load_file_to_table(File(filepath, filetype=FileType.CSV), target_table, {})

    df = database.hook.get_pandas_df(f"SELECT * FROM {database.get_table_qualified_name(target_table)}")
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
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_load_file_from_cloud_to_table(database_table_fixture):
    """Test loading on files to snowflake database"""
    database, target_table = database_table_fixture
    database.load_file_to_table(
        File("s3://astro-sdk/data/", conn_id="aws_conn", filetype=FileType.CSV),
        target_table,
        {},
    )

    df = database.hook.get_pandas_df(f"SELECT * FROM {database.get_table_qualified_name(target_table)}")
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
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
@mock.patch("astro.databases.snowflake.SnowflakeDatabase.hook")
@mock.patch("astro.databases.snowflake.SnowflakeDatabase.create_stage")
def test_load_file_to_table_natively_for_fallback_raises_exception_if_not_enable_native_fallback(
    mock_stage, mock_hook, database_table_fixture
):
    """Test loading on files to snowflake natively for fallback raise exception."""
    mock_hook.run.side_effect = ValueError
    mock_stage.return_value = SnowflakeStage(
        name="mock_stage",
        url="gcs://bucket/prefix",
        metadata=Metadata(database="SNOWFLAKE_DATABASE", schema="SNOWFLAKE_SCHEMA"),
    )
    database, target_table = database_table_fixture
    filepath = str(pathlib.Path(CWD.parent, "data/sample.csv"))
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
            "database": Database.SNOWFLAKE,
            "table": Table(name="test_table", metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
)
@mock.patch("astro.databases.snowflake.is_valid_snow_identifier")
def test_build_merge_sql(mock_is_valid_snow_identifier, database_table_fixture):
    """Test build merge SQL for DatabaseCustomError"""
    mock_is_valid_snow_identifier.return_value = False
    database, target_table = database_table_fixture
    with pytest.raises(DatabaseCustomError):
        database._build_merge_sql(
            source_table=Table(name="source_test_table", metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
            target_table=target_table,
            source_to_target_columns_map={"list": "val"},
            target_conflict_columns=["target"],
        )


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        },
    ],
    indirect=True,
    ids=["snowflake"],
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
            "database": Database.SNOWFLAKE,
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            "table": Table(
                metadata=Metadata(
                    schema=os.getenv("SNOWFLAKE_SCHEMA", SNOWFLAKE_SCHEMA),
                    database=os.getenv("SNOWFLAKE_DATABASE", "snowflake"),
                )
            ),
        },
    ],
    indirect=True,
    ids=["snowflake"],
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
    [
        {
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
        }
    ],
    indirect=True,
    ids=["snowflake"],
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
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [{"provider": "google", "file_create": False}],
    indirect=True,
    ids=["google"],
)
def test_export_table_to_file_in_the_cloud(database_table_fixture, remote_files_fixture):
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
            "database": Database.SNOWFLAKE,
            "table": Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA)),
            "file": File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
        }
    ],
    indirect=True,
    ids=["snowflake"],
)
def test_create_table_from_select_statement(database_table_fixture):
    """Test table creation via select statement"""
    database, original_table = database_table_fixture

    statement = f'SELECT * FROM {database.get_table_qualified_name(original_table)} WHERE "id" = 1;'
    target_table = Table(metadata=Metadata(schema=SNOWFLAKE_SCHEMA))
    database.create_table_from_select_statement(statement, target_table)

    df = database.hook.get_pandas_df(f"SELECT * FROM {database.get_table_qualified_name(target_table)}")
    assert len(df) == 1
    expected = pd.DataFrame([{"id": 1, "name": "First"}])
    test_utils.assert_dataframes_are_equal(df, expected)
    database.drop_table(target_table)


def test_stage_set_name_after():
    stage = SnowflakeStage()
    stage.name = "abc"
    assert stage.name == "abc"


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [
        {"provider": "google", "filetype": FileType.CSV},
    ],
    indirect=True,
    ids=["google_csv"],
)
def test_stage_exists_false(remote_files_fixture):
    file_fixture = File(remote_files_fixture[0])
    database = SnowflakeDatabase(conn_id=CUSTOM_CONN_ID)
    stage = SnowflakeStage(
        name="inexistent-stage",
        metadata=database.default_metadata,
    )
    stage.set_url_from_file(file_fixture)
    assert not database.stage_exists(stage)


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [
        {"provider": "google", "filetype": FileType.CSV},
        {"provider": "google", "filetype": FileType.NDJSON},
        {"provider": "google", "filetype": FileType.PARQUET},
        {"provider": "amazon", "filetype": FileType.CSV},
    ],
    indirect=True,
    ids=["google_csv", "google_ndjson", "google_parquet", "amazon_csv"],
)
def test_create_stage_succeeds_with_storage_integration(remote_files_fixture):
    file_fixture = File(remote_files_fixture[0])

    if file_fixture.location.location_type == FileLocation.GS:
        storage_integration = SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE
    else:
        storage_integration = SNOWFLAKE_STORAGE_INTEGRATION_AMAZON

    database = SnowflakeDatabase(conn_id=CUSTOM_CONN_ID)
    stage = database.create_stage(file=file_fixture, storage_integration=storage_integration)
    assert database.stage_exists(stage)
    database.drop_stage(stage)


@pytest.mark.integration
@pytest.mark.parametrize(
    "remote_files_fixture",
    [
        {"provider": "amazon", "filetype": FileType.CSV},
    ],
    indirect=True,
    ids=["amazon_csv"],
)
def test_create_stage_succeeds_without_storage_integration(remote_files_fixture):
    file_fixture = File(remote_files_fixture[0])
    database = SnowflakeDatabase(conn_id=CUSTOM_CONN_ID)
    stage = database.create_stage(file=file_fixture, storage_integration=None)
    assert database.stage_exists(stage)
    database.drop_stage(stage)


def test_create_stage_google_fails_due_to_no_storage_integration():
    database = SnowflakeDatabase(conn_id="fake-conn")
    with pytest.raises(ValueError) as exc_info:
        database.create_stage(file=File("gs://some-bucket/some-file.csv"))
    expected_msg = "In order to create an stage for GCS, `storage_integration` is required."
    assert exc_info.match(expected_msg)


class MockCredentials:
    access_key = None
    secret_key = None


@patch(
    "astro.files.locations.amazon.s3.S3Hook.get_credentials",
    return_value=MockCredentials(),
)
def test_create_stage_amazon_fails_due_to_no_credentials(get_credentials):
    database = SnowflakeDatabase(conn_id="fake-conn")
    with pytest.raises(ValueError) as exc_info:
        database.create_stage(file=File("s3://some-bucket/some-file.csv"))
    expected_msg = "In order to create an stage for S3, one of the following is required"
    assert exc_info.match(expected_msg)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SNOWFLAKE},
    ],
    indirect=True,
    ids=["snowflake"],
)
@pytest.mark.parametrize(
    "remote_files_fixture",
    [
        {"provider": "amazon", "filetype": FileType.CSV},
    ],
    indirect=True,
    ids=["amazon_csv"],
)
def test_load_file_to_table_natively(remote_files_fixture, database_table_fixture):
    """Load a file to a Snowflake table using the native optimisation."""
    filepath = remote_files_fixture[0]
    database, target_table = database_table_fixture
    database.load_file_to_table(File(filepath), target_table, {}, use_native_support=True)

    df = database.hook.get_pandas_df(f"SELECT * FROM {target_table.name}")
    assert len(df) == 3
    expected = pd.DataFrame(
        [
            {"id": 1, "name": "First"},
            {"id": 2, "name": "Second"},
            {"id": 3, "name": "Third with unicode पांचाल"},
        ]
    )
    test_utils.assert_dataframes_are_equal(df, expected)


def test_snowflake_file_format_create_unique_name():
    """
    Test if file format is being set properly.
    """
    snowflake_file_format = SnowflakeFileFormat(name="file_format", file_type="PARQUET")
    assert snowflake_file_format.name == "file_format"
