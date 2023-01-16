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


def test_stage_set_name_after():
    stage = SnowflakeStage()
    stage.name = "abc"
    assert stage.name == "abc"


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
    path = str(CWD) + "/../../data/homes_main.csv"
    database = SnowflakeDatabase(
        conn_id="fake-conn", load_options=SnowflakeLoadOptions(file_options={"foo": "bar"})
    )
    file = File(path)
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
    assert "COPY_OPTIONS=(ON_ERROR=CONTINUE)" in database.run_sql.call_args[0][0]


def test_snowflake_load_options_default():
    path = str(CWD) + "/../../data/homes_main.csv"
    database = SnowflakeDatabase(conn_id="fake-conn", load_options=SnowflakeLoadOptions())
    file = File(path)
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
    assert "COPY_OPTIONS=(ON_ERROR=CONTINUE)" in database.run_sql.call_args[0][0]


def test_snowflake_load_options_wrong_options():
    path = str(CWD) + "/../../data/homes_main.csv"
    file = File(path)
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


def test_storage_integrations_params():
    """
    test that if `storage_integration` is not passed to `load_file_to_table_natively` method either via
    `native_support_kwargs` or via `SnowflakeLoadOptions` it raises an exception.
    """
    table = Table(conn_id="fake-conn")
    file = File(path="azure://data/homes_main.ndjson")
    database = SnowflakeDatabase(conn_id=table.conn_id, load_options=SnowflakeLoadOptions())
    with pytest.raises(ValueError) as e:
        database.load_file_to_table_natively(source_file=file, target_table=table)
    assert e.match(
        "Param 'storage_integration' is required. Please pass either in `native_support_kwargs` or"
        " `SnowflakeLoadOptions.copy_options`"
    )


def test_storage_integrations_params_in_native_support_kwargs():
    """
    Test passing of storage_integrations in native_support_kwargs
    """
    table = Table(conn_id="fake-conn")
    file = File(path="azure://data/homes_main.ndjson")
    database = SnowflakeDatabase(conn_id=table.conn_id, load_options=SnowflakeLoadOptions())

    with mock.patch("astro.databases.snowflake.SnowflakeDatabase.create_stage"), mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase._copy_into_table_from_stage"
    ) as _copy_into_table_from_stage, mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase.evaluate_results"
    ):
        database.load_file_to_table_natively(
            source_file=file,
            target_table=table,
            native_support_kwargs={"storage_integration": "some_integrations"},
        )


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

    with mock.patch("astro.databases.snowflake.SnowflakeDatabase.create_stage"), mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase._copy_into_table_from_stage"
    ) as _copy_into_table_from_stage, mock.patch(
        "astro.databases.snowflake.SnowflakeDatabase.evaluate_results"
    ):
        database.load_file_to_table_natively(source_file=file, target_table=table)
