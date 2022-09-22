import importlib
import os
from unittest import mock

import pytest
from astro.constants import (
    SUPPORTED_DATABASES,
    SUPPORTED_FILE_LOCATIONS,
    SUPPORTED_FILE_TYPES,
)


def test_supported_file_locations():
    expected = ["gs", "http", "https", "local", "s3"]
    assert sorted(SUPPORTED_FILE_LOCATIONS) == expected


def test_supported_file_types():
    expected = ["csv", "json", "ndjson", "parquet"]
    assert sorted(SUPPORTED_FILE_TYPES) == expected


def test_supported_databases():
    expected = ["bigquery", "postgres", "redshift", "snowflake", "sqlite"]
    assert sorted(SUPPORTED_DATABASES) == expected


@mock.patch.dict(
    os.environ, {"AIRFLOW__CORE__ENABLE_XCOM_PICKLING": "False"}, clear=True
)
def test_enable_xcom_pickling_set_false_in_env():
    import astro

    with pytest.raises(OSError) as exe_info:
        importlib.reload(astro)

    assert (
        exe_info.value.args[0]
        == "AIRFLOW__CORE__ENABLE_XCOM_PICKLING environment variable needs to be set to True or "
        "enable_xcom_pickling=true in airflow.cfg before importing astro-sdk-python."
    )
