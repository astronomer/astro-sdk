from astro.constants import SUPPORTED_DATABASES, SUPPORTED_FILE_LOCATIONS, SUPPORTED_FILE_TYPES


def test_supported_file_locations():
    expected = {"ftp", "gdrive", "gs", "http", "https", "local", "s3", "sftp", "wasb", "wasbs", "azure"}
    assert set(SUPPORTED_FILE_LOCATIONS) == expected


def test_supported_file_types():
    expected = {"csv", "json", "ndjson", "parquet", "xls", "xlsx"}
    assert set(SUPPORTED_FILE_TYPES) == expected


def test_supported_databases():
    expected = {
        "bigquery",
        "delta",
        "duckdb",
        "mssql",
        "mysql",
        "postgres",
        "redshift",
        "snowflake",
        "sqlite",
    }
    assert set(SUPPORTED_DATABASES) == expected
