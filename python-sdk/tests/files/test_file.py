import pathlib
import pickle
from datetime import datetime
from unittest.mock import patch

import pandas as pd
import pytest
from airflow import DAG

from astro import constants
from astro.dataframes.load_options import (
    PandasCsvLoadOptions,
    PandasJsonLoadOptions,
    PandasLoadOptions,
    PandasNdjsonLoadOptions,
    PandasParquetLoadOptions,
)
from astro.files import File, get_file_list, resolve_file_path_pattern
from astro.options import SnowflakeLoadOptions, WASBLocationLoadOptions

sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent, "data/sample.csv")
sample_filepaths_per_filetype = [
    (False, "/tmp/sample.csv"),
    (False, "/tmp/sample.json"),
    (False, "/tmp/sample.ndjson"),
    (True, "/tmp/sample.parquet"),
    (True, "/tmp/sample.xlsx"),
]


@pytest.mark.parametrize(
    "filetype",
    [
        (False, "/tmp/sample.csv"),
        (False, "/tmp/sample.json"),
        (False, "/tmp/sample.ndjson"),
        (True, "/tmp/sample.parquet"),
    ],
    ids=["csv", "json", "ndjson", "parquet"],
)
def test_is_binary(filetype):
    """Test if the file is of text/binary format"""
    assert File(filetype[1]).is_binary() == filetype[0]


@pytest.mark.parametrize(
    "filetype",
    [
        (False, "/tmp/sample.csv"),
        (False, "/tmp/sample.json"),
        (False, "/tmp/sample.ndjson"),
        (False, "/tmp/sample.parquet"),
        (False, "/tmp/sample.xlsx"),
        (True, "/tmp/"),
        (True, "s3://tmp/home_*"),
        (False, "s3://tmp/.folder/sample.csv"),
        (True, "s3://tmp/.folder/"),
    ],
    ids=["csv", "json", "ndjson", "parquet", "xlsx", "csv", "json", "csv", "json"],
)
def test_is_pattern(filetype):
    """Test if the file is a file pattern"""
    assert File(filetype[1]).is_pattern() == filetype[0]


def test_path_property():
    """Test path property is having correct path"""
    path = "/tmp/sample.csv"
    assert File(path).path == path


@pytest.mark.parametrize(
    "invalid_path",
    [
        "/tmp/cklcdklscdksl.csv",
        "/tmp/cklcdklscdksl/*.csv",
    ],
)
def test_resolve_file_path_pattern_raise_exception(invalid_path):
    """resolve_file_path_pattern expected to fail with default 'if_file_doesnt_exist' exception strategy"""

    with pytest.raises(FileNotFoundError) as e:
        _ = resolve_file_path_pattern(path_pattern=invalid_path)
    expected_error = f"File(s) not found for path/pattern '{invalid_path}'"
    assert expected_error in str(e.value)


def test_get_file_list():
    """Assert that get_file_list handle kwargs correctly"""
    dag = DAG(dag_id="dag1", start_date=datetime(2022, 1, 1))

    resp = get_file_list(path="path", conn_id="conn", dag=dag)
    assert resp.operator.task_id == "get_file_list"

    resp = get_file_list(path="path", conn_id="conn", dag=dag)
    assert resp.operator.task_id != "get_file_list"

    resp = get_file_list(path="path", conn_id="conn", task_id="test")
    assert resp.operator.task_id == "test"


@pytest.mark.parametrize(
    "file_1,file_2,equality",
    [
        (File("/tmp/file_a.csv"), File("/tmp/file_a.csv"), True),
        (
            File("gs://tmp/file_a.csv", conn_id="google_cloud_default"),
            File("gs://tmp/file_a.csv", conn_id="google_cloud_default"),
            True,
        ),
        (
            File("gs://tmp/file_a.csv", conn_id="google_cloud_default", filetype=constants.FileType.CSV),
            File("gs://tmp/file_a.csv", conn_id="google_cloud_default", filetype=constants.FileType.CSV),
            True,
        ),
        (
            File("gs://tmp/file_a.csv", conn_id="google_cloud_default", filetype=constants.FileType.CSV),
            File("gs://tmp/file_a.csv", conn_id="google_cloud_default", filetype=constants.FileType.JSON),
            False,
        ),
        (
            File("gs://tmp/file_a.csv", conn_id="google_cloud_default", filetype=constants.FileType.CSV),
            File("gs://tmp/file_b.csv", conn_id="google_cloud_default", filetype=constants.FileType.CSV),
            False,
        ),
    ],
)
def test_file_eq(file_1, file_2, equality):
    """Test that equality works"""
    if equality:
        assert file_1 == file_2
    else:
        assert file_1 != file_2


def test_file_hash():
    """Test that hashing works"""
    assert isinstance(hash(File("/tmp/file_a.csv")), int)


@pytest.mark.parametrize(
    "table,dataset_uri",
    [
        (File(path="gs://bucket/object/path"), "astro+gs://bucket/object/path"),
        (
            File(path="gs://bucket/object/path", conn_id="test_conn"),
            "astro+gs://test_conn@bucket/object/path",
        ),
        (
            File(
                path="gs://bucket/object/path",
                conn_id="test_conn",
                filetype=constants.FileType.CSV,
            ),
            "astro+gs://test_conn@bucket/object/path?filetype=csv",
        ),
        (
            File(path=str(pathlib.Path(pathlib.Path(__file__).parent.parent, "data/sample.csv"))),
            f"astro+file:{str(pathlib.Path(pathlib.Path(__file__).parent.parent, 'data/sample.csv'))}",
        ),
        (
            File(path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"),
            "astro+https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv",
        ),
    ],
)
def test_file_to_datasets_uri(table, dataset_uri):
    """Verify that Table build and pass correct URI"""
    assert table.uri == dataset_uri


def test_file_to_datasets_extra():
    """Verify that extra is set"""
    table = File(path="gs://bucket/object/path", conn_id="test_conn")
    assert table.extra == {}


@pytest.mark.parametrize(
    "files",
    [
        {
            "path": "data/sample.csv",
            "_convert_remote_file_to_byte_stream": False,
        },
        {
            "path": "data/sample.ndjson",
            "_convert_remote_file_to_byte_stream": False,
        },
        {
            "path": "data/sample.parquet",
            "_convert_remote_file_to_byte_stream": True,
        },
    ],
    ids=["csv", "ndjson", "parquet"],
)
def test_smart_open_file_stream_only_conveted_to_BytesIO_buffer_for_parquet(files):
    """
    Verify that the fix for https://github.com/RaRe-Technologies/smart_open/issues/524)
     is only applied in case of parquet files
    """
    file = files["path"]
    _convert_remote_file_to_byte_stream_called = files["_convert_remote_file_to_byte_stream"]
    path = str(pathlib.Path(pathlib.Path(__file__).parent.parent, file))
    sample_file_object = File(path)
    with patch(
        "astro.files.types.parquet.ParquetFileType._convert_remote_file_to_byte_stream"
    ) as _convert_remote_file_to_byte_stream, patch(
        "astro.files.types.parquet.pd.read_parquet"
    ) as read_parquet:
        read_parquet.return_value = pd.DataFrame([1, 2])
        sample_file_object.export_to_dataframe()
        if _convert_remote_file_to_byte_stream_called:
            _convert_remote_file_to_byte_stream.assert_called()
        else:
            _convert_remote_file_to_byte_stream.assert_not_called()


def test_if_file_object_can_be_pickled():
    """Verify if we can pickle File object"""
    file = File(path="./test.csv")
    assert pickle.loads(pickle.dumps(file)) == file


@pytest.mark.parametrize(
    "file_type",
    [
        {"type": "csv", "expected_class": PandasLoadOptions},
        {"type": "ndjson", "expected_class": PandasLoadOptions},
        {"type": "json", "expected_class": PandasLoadOptions},
        {"type": "parquet", "expected_class": PandasLoadOptions},
        {"type": "xlsx", "expected_class": PandasLoadOptions},
    ],
    ids=["csv", "ndjson", "json", "parquet", "xlsx"],
)
@pytest.mark.parametrize(
    "file_location",
    [
        {"location": "s3://dummy/test", "expected_class": None},
        {"location": "gs://dummy/test", "expected_class": None},
        # ToDo: Get the correct protocol for the azure URL. `wasb, wasbs, azure` is failing.
        # {
        #     "location": "wasb://dummy/test",
        #     "expected_class": None
        # },
        {"location": "ftp://dummy/test", "expected_class": None},
        {"location": "sftp://dummy/test", "expected_class": None},
        {"location": "gdrive://dummy/test", "expected_class": None},
        {"location": "http://dummy.com/test", "expected_class": None},
        {"location": "https://dummy.com/test", "expected_class": None},
        {"location": "./test", "expected_class": None},  # local path
    ],
    ids=["s3", "gs", "ftp", "sftp", "gdrive", "http", "https", "local"],
)
def test_file_object_picks_load_options(file_type, file_location):
    """Test file object pick correct load_options"""
    type_name, type_expected_class = file_type.values()
    location_path, location_expected_class = file_location.values()
    file = File(
        path=location_path + f".{type_name}",
    )
    file.load_options = [
        PandasLoadOptions(delimiter="$"),
        SnowflakeLoadOptions(copy_options={}),
        WASBLocationLoadOptions(storage_account="some_account"),
    ]
    assert type(file.type.load_options) is type_expected_class
    assert file.location.load_options is location_expected_class


@pytest.mark.parametrize(
    "file_type",
    [
        {"type": "csv", "expected_class": PandasCsvLoadOptions},
        {"type": "ndjson", "expected_class": PandasNdjsonLoadOptions},
        {"type": "json", "expected_class": PandasJsonLoadOptions},
        {"type": "parquet", "expected_class": PandasParquetLoadOptions},
    ],
    ids=["csv", "ndjson", "json", "parquet"],
)
@pytest.mark.parametrize(
    "file_location",
    [
        {"location": "s3://dummy/test", "expected_class": None},
        {"location": "gs://dummy/test", "expected_class": None},
        {"location": "ftp://dummy/test", "expected_class": None},
        {"location": "sftp://dummy/test", "expected_class": None},
        {"location": "gdrive://dummy/test", "expected_class": None},
        {"location": "http://dummy.com/test", "expected_class": None},
        {"location": "https://dummy.com/test", "expected_class": None},
        {"location": "./test", "expected_class": None},  # local path
    ],
    ids=["s3", "gs", "ftp", "sftp", "gdrive", "http", "https", "local"],
)
def test_file_object_picks_load_options_with_deprecated_load_options(file_type, file_location):
    """Test file object pick correct load_options"""
    type_name, type_expected_class = file_type.values()
    location_path, _ = file_location.values()
    file = File(path=location_path + f".{type_name}")
    file.load_options = [
        PandasCsvLoadOptions(delimiter="$"),
        PandasJsonLoadOptions(encoding="test"),
        PandasParquetLoadOptions(columns=["name", "age"]),
        PandasNdjsonLoadOptions(normalize_sep="__"),
    ]
    assert type(file.type.load_options) is type_expected_class
