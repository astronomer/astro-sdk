from astro.constants import SUPPORTED_FILE_LOCATIONS, SUPPORTED_FILE_TYPES


def test_supported_file_locations():
    expected = ["gs", "http", "https", "local", "s3"]
    assert sorted(SUPPORTED_FILE_LOCATIONS) == expected


def test_supported_file_types():
    expected = ["csv", "json", "ndjson", "parquet"]
    assert sorted(SUPPORTED_FILE_TYPES) == expected
