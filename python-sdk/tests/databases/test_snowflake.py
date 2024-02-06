"""Tests specific to the Snowflake Database implementation."""

import pathlib
from unittest import mock
from unittest.mock import MagicMock, PropertyMock, patch

import pytest

from astro.databases.snowflake import SnowflakeDatabase, SnowflakeFileFormat, SnowflakeStage
from astro.exceptions import DatabaseCustomError
from astro.files import File
from astro.options import LoadOptions, SnowflakeLoadOptions
from astro.settings import SNOWFLAKE_STORAGE_INTEGRATION_AMAZON, SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE
from astro.table import Metadata, Table

DEFAULT_CONN_ID = "snowflake_default"
CUSTOM_CONN_ID = "snowflake_conn"
SUPPORTED_CONN_IDS = [CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent

SNOWFLAKE_STORAGE_INTEGRATION_AMAZON = SNOWFLAKE_STORAGE_INTEGRATION_AMAZON or "aws_int_python_sdk"
SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE = SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE or "gcs_int_python_sdk"

LOCAL_CSV_FILE = str(CWD.parent / "data/homes_main.csv")


def test_stage_set_name_after():
    stage = SnowflakeStage()
    stage.name = "abc"
    assert stage.name == "abc"


def test_create_stage_google_fails_due_to_no_storage_integration():
    database = SnowflakeDatabase(conn_id="fake-conn")
    with pytest.raises(ValueError) as exc_info:
        database.create_stage(file=File("gs://some-bucket/some-file.csv"))
    expected_msg = "In order to create a stage, `storage_integration` is required."
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


def test_snowflake_file_format_create_unique_name():
    """
    Test if file format is being set properly.
    """
    snowflake_file_format = SnowflakeFileFormat(name="file_format", file_type="PARQUET")
    assert snowflake_file_format.name == "file_format"


@pytest.mark.parametrize(
    "cols_eval",
    [
        # {"cols": ["SELL", "LIST"], "expected_result": False},
        {"cols": ["Sell", "list"], "expected_result": True},
        {"cols": ["sell", "List"], "expected_result": True},
        {"cols": ["sell", "lIst"], "expected_result": True},
        {"cols": ["sEll", "list"], "expected_result": True},
        {"cols": ["sell", "LIST"], "expected_result": False},
        {"cols": ["sell", "list"], "expected_result": False},
    ],
)
def test_use_quotes(cols_eval):
    """
    Verify the quotes addition only in case where we are having mixed case col names
    """
    assert SnowflakeDatabase.use_quotes(cols_eval["cols"]) == cols_eval["expected_result"]


@mock.patch("astro.databases.snowflake.SnowflakeDatabase.hook")
@mock.patch("astro.databases.snowflake.SnowflakeDatabase.create_stage")
def test_load_file_to_table_natively_for_fallback_raises_exception_if_not_enable_native_fallback(
    mock_stage, mock_hook
):
    mock_hook.run.side_effect = [
        ValueError,  # 1st run call copies the data
        None,  # 2nd run call drops the stage
    ]
    mock_stage.return_value = SnowflakeStage(
        name="mock_stage",
        url="gcs://bucket/prefix",
        metadata=Metadata(database="SNOWFLAKE_DATABASE", schema="SNOWFLAKE_SCHEMA"),
    )
    database = SnowflakeDatabase()
    database.load_options = SnowflakeLoadOptions(
        copy_options={"ON_ERROR": "CONTINUE"},
        file_options={"TYPE": "CSV", "TRIM_SPACE": True},
    )
    with pytest.raises(DatabaseCustomError):
        database.load_file_to_table_natively_with_fallback(
            source_file=File(str(pathlib.Path(CWD.parent, "data/sample.csv"))),
            target_table=Table(),
        )
    mock_hook.run.assert_has_calls(
        [
            mock.call(f"DROP STAGE IF EXISTS {mock_stage.return_value.qualified_name};", autocommit=True),
        ]
    )


def test_snowflake_load_options():
    database = SnowflakeDatabase(
        conn_id="fake-conn", load_options=SnowflakeLoadOptions(file_options={"foo": "bar"})
    )
    file = File(path=LOCAL_CSV_FILE)
    with mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase.hook", new_callable=PropertyMock
    ), mock.patch(
        "astro.databases.snowflake.SnowflakeStage.qualified_name", new_callable=PropertyMock
    ) as mock_q_name:
        mock_q_name.return_value = "foo"
        database.run_sql = MagicMock()
        database.create_stage(
            file=file,
            storage_integration="foo",
        )
    assert "FILE_FORMAT=(foo=bar, TYPE=CSV, TRIM_SPACE=TRUE)" in database.run_sql.call_args[0][0]
    assert "COPY_OPTIONS=()" in database.run_sql.call_args[0][0]


def test_snowflake_load_options_default():
    database = SnowflakeDatabase(conn_id="fake-conn", load_options=SnowflakeLoadOptions())
    file = File(path=LOCAL_CSV_FILE)
    with mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase.hook", new_callable=PropertyMock
    ), mock.patch(
        "astro.databases.snowflake.SnowflakeStage.qualified_name", new_callable=PropertyMock
    ) as mock_q_name:
        mock_q_name.return_value = "foo"
        database.run_sql = MagicMock()
        database.create_stage(
            file=file,
            storage_integration="foo",
        )
    assert "FILE_FORMAT=(TYPE=CSV, TRIM_SPACE=TRUE)" in database.run_sql.call_args[0][0]
    assert "COPY_OPTIONS=()" in database.run_sql.call_args[0][0]


def test_snowflake_load_options_metadata_columns_create_table():
    database = SnowflakeDatabase(
        conn_id="fake-conn", load_options=SnowflakeLoadOptions(metadata_columns=["METADATA$FILENAME"])
    )
    database.create_table_using_schema_autodetection = MagicMock()
    database.hook = MagicMock()
    with mock.patch(
        "astro.databases.snowflake.SnowflakeStage.qualified_name", new_callable=PropertyMock
    ) as mock_q_name:
        mock_q_name.return_value = "foo"
        table = Table()
        database.create_table(table)
    expected_sql = f"ALTER TABLE {table.name} ADD COLUMN IF NOT EXISTS METADATA$FILENAME VARCHAR;"
    assert database.hook.run.call_args_list[0].args[0] == expected_sql


def test_snowflake_load_options_wrong_options():
    file = File(path=LOCAL_CSV_FILE)
    with pytest.raises(ValueError, match="Error: Requires a SnowflakeLoadOptions"):
        database = SnowflakeDatabase(conn_id="fake-conn", load_options=LoadOptions())
        database.load_file_to_table_natively(source_file=file, target_table=Table())


def test_snowflake_load_options_empty():
    load_options = SnowflakeLoadOptions()
    assert load_options.empty()
    load_options.copy_options = {"foo": "bar"}
    assert not load_options.empty()
    load_options.file_options = {"biz": "baz"}
    assert not load_options.empty()
    load_options.copy_options = {}
    assert not load_options.empty()
    load_options.file_options = {}
    assert load_options.empty()


def test_storage_integrations_params_in_native_support_kwargs():
    """
    Test passing of storage_integrations in native_support_kwargs
    """
    table = Table(conn_id="fake-conn")
    file = File(path="azure://data/homes_main.ndjson")
    database = SnowflakeDatabase(conn_id=table.conn_id, load_options=SnowflakeLoadOptions())

    with mock.patch("astro.databases.snowflake.SnowflakeDatabase.create_stage") as create_stage, mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase._copy_into_table_from_stage"
    ) as _copy_into_table_from_stage, mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase.evaluate_results"
    ):
        database.load_file_to_table_natively(
            source_file=file,
            target_table=table,
            native_support_kwargs={"storage_integration": "some_integrations"},
        )
    assert create_stage.call_args.kwargs["storage_integration"] == "some_integrations"


def test_storage_integrations_params_in_load_options():
    """
    Test passing of storage_integrations in SnowflakeLoadOptions
    """
    table = Table(conn_id="fake-conn")
    file = File(path="azure://data/homes_main.ndjson")
    database = SnowflakeDatabase(
        conn_id=table.conn_id,
        load_options=SnowflakeLoadOptions(storage_integration="some_integrations"),
    )

    with mock.patch("astro.databases.snowflake.SnowflakeDatabase.create_stage") as create_stage, mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase._copy_into_table_from_stage"
    ) as _copy_into_table_from_stage, mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase.evaluate_results"
    ):
        database.load_file_to_table_natively(source_file=file, target_table=table)

    assert create_stage.call_args.kwargs["storage_integration"] == "some_integrations"


def test_load_file_to_table_by_default_checks_schema():
    database = SnowflakeDatabase(conn_id="fake-conn")
    database.run_sql = MagicMock()
    database.hook = MagicMock()
    database.create_table_using_schema_autodetection = MagicMock()

    file_ = File(path=LOCAL_CSV_FILE)
    table = Table(conn_id="fake-conn", metadata=Metadata(schema="abc"))
    database.load_file_to_table(input_file=file_, output_table=table)
    expected = (
        "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;"
    )
    assert database.hook.run.call_args_list[0].args[0] == expected
    assert database.hook.run.call_args_list[0].kwargs["parameters"]["schema_name"] == "abc"


def test_load_file_to_table_skips_schema_check():
    database = SnowflakeDatabase(conn_id="fake-conn")
    database.run_sql = MagicMock()
    database.hook = MagicMock()
    database.create_table_using_schema_autodetection = MagicMock()

    file_ = File(path=LOCAL_CSV_FILE)
    table = Table(conn_id="fake-conn", metadata=Metadata(schema="abc"))
    database.load_file_to_table(input_file=file_, output_table=table, assume_schema_exists=True)
    assert not database.hook.run.call_count


def test_get_copy_into_with_metadata_sql_statement():
    database = SnowflakeDatabase(
        conn_id="fake-conn", load_options=SnowflakeLoadOptions(metadata_columns=["METADATA$FILENAME"])
    )
    database.hook = MagicMock()
    file_path = File(path=LOCAL_CSV_FILE).path
    table = Table(conn_id="fake-conn", metadata=Metadata(schema="abc"))
    stage = SnowflakeStage(
        name="mock_stage",
        url="gcs://bucket/prefix",
        metadata=Metadata(database="SNOWFLAKE_DATABASE", schema="SNOWFLAKE_SCHEMA"),
    )

    sql_statement = database._get_copy_into_with_metadata_sql_statement(file_path, table, stage)
    exp_sq = f"COPY INTO {table.name.upper()} FROM (SELECT METADATA$FILENAME FROM @{stage.qualified_name}/{file_path}) "
    assert sql_statement == exp_sq


def test_get_copy_into_with_metadata_sql_statement_no_metadata_columns():
    database = SnowflakeDatabase(conn_id="fake-conn", load_options=SnowflakeLoadOptions())
    database.hook = MagicMock()
    file_path = File(path=LOCAL_CSV_FILE).path
    table = Table(conn_id="fake-conn", metadata=Metadata(schema="abc"))
    stage = SnowflakeStage(
        name="mock_stage",
        url="gcs://bucket/prefix",
        metadata=Metadata(database="SNOWFLAKE_DATABASE", schema="SNOWFLAKE_SCHEMA"),
    )
    with pytest.raises(ValueError, match="Error: Requires metadata columns to be set in load options"):
        database._get_copy_into_with_metadata_sql_statement(file_path, table, stage)
