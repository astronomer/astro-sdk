from __future__ import annotations

from astro.constants import SUPPORTED_DATABASES, SUPPORTED_FILE_LOCATIONS, SUPPORTED_FILE_TYPES


def test_supported_file_locations():
    expected = ["gdrive", "gs", "http", "https", "local", "s3", "wasb", "wasbs"]
    assert sorted(SUPPORTED_FILE_LOCATIONS) == expected


def test_supported_file_types():
    expected = ["csv", "json", "ndjson", "parquet"]
    assert sorted(SUPPORTED_FILE_TYPES) == expected


def test_supported_databases():
    expected = ["bigquery", "delta", "postgres", "redshift", "snowflake", "sqlite"]
    assert sorted(SUPPORTED_DATABASES) == expected
