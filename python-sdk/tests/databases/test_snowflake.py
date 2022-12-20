"""Tests specific to the Snowflake Database implementation."""
from __future__ import annotations

import pathlib
from unittest.mock import patch

import pytest

from astro.databases.snowflake import SnowflakeDatabase, SnowflakeFileFormat, SnowflakeStage
from astro.files import File
from astro.settings import SNOWFLAKE_STORAGE_INTEGRATION_AMAZON, SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE

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
