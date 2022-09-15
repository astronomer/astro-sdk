import pathlib
from datetime import datetime
from unittest.mock import mock_open, patch

import pandas as pd
import pytest
from airflow import DAG
from astro import constants
from astro.constants import SUPPORTED_FILE_TYPES, FileType
from astro.files import File, get_file_list, resolve_file_path_pattern
from botocore.client import BaseClient
from google.cloud.storage import Client

sample_file = pathlib.Path(pathlib.Path(__file__).parent.parent, "data/sample.csv")
sample_filepaths_per_filetype = [
    (False, "/tmp/sample.csv"),
    (False, "/tmp/sample.json"),
    (False, "/tmp/sample.ndjson"),
    (True, "/tmp/sample.parquet"),
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


def test_path_property():
    """Test path property is having correct path"""
    path = "/tmp/sample.csv"
    assert File(path).path == path


@pytest.mark.parametrize(
    "files",
    [
        {
            "path": "/tmp/sample.csv",
            "instance": None,
        },
        {
            "path": "s3://tmp/sample.csv",
            "instance": BaseClient,
        },
        {
            "path": "gs://tmp/sample.csv",
            "instance": Client,
        },
    ],
)
@patch("astro.files.locations.base.smart_open.open")
def test_exists(mocked_smart_open, files):
    """Test existence of files across supported locations"""
    File(files["path"]).exists()
    mocked_smart_open.assert_called()
    kwargs = mocked_smart_open.call_args.kwargs
    args = mocked_smart_open.call_args.args
    if kwargs["transport_params"]:
        assert isinstance(kwargs["transport_params"]["client"], files["instance"])
    assert files["path"] == args[0]


@pytest.mark.parametrize(
    "type_method_map_fixture", [{"method": "create_from_dataframe"}], indirect=True
)
@pytest.mark.parametrize(
    "locations",
    [
        {
            "path": "/tmp/sample",
            "instance": None,
        },
        {
            "path": "s3://tmp/sample",
            "instance": BaseClient,
        },
        {
            "path": "gs://tmp/sample",
            "instance": Client,
        },
    ],
    ids=["local", "s3", "gcs"],
)
@pytest.mark.parametrize("filetype", SUPPORTED_FILE_TYPES)
@patch("astro.files.base.smart_open.open")
def test_create_from_dataframe(
    mocked_smart_open, filetype, locations, type_method_map_fixture
):
    """Test create_from_dataframe() for all locations and filetypes"""
    data = {"id": [1, 2, 3], "name": ["First", "Second", "Third with unicode पांचाल"]}
    df = pd.DataFrame(data=data)
    with patch(type_method_map_fixture[FileType(filetype)]) as mocked_write:

        path = locations["path"] + "." + filetype

        File(path).create_from_dataframe(df=df)
        mocked_smart_open.assert_called()
        kwargs = mocked_smart_open.call_args.kwargs
        args = mocked_smart_open.call_args.args
        if kwargs["transport_params"]:
            assert isinstance(
                kwargs["transport_params"]["client"], locations["instance"]
            )
        assert path == args[0]

        mocked_write.assert_called()
        mocked_write.stop()


@pytest.mark.parametrize(
    "type_method_map_fixture", [{"method": "export_to_dataframe"}], indirect=True
)
@pytest.mark.parametrize(
    "locations",
    [
        {
            "path": "/tmp/sample",
            "instance": None,
        },
        {
            "path": "s3://tmp/sample",
            "instance": BaseClient,
        },
        {
            "path": "gs://tmp/sample",
            "instance": Client,
        },
    ],
    ids=["local", "s3", "gcs"],
)
@pytest.mark.parametrize("filetype", SUPPORTED_FILE_TYPES)
def test_export_to_dataframe(filetype, locations, type_method_map_fixture):
    """Test export_to_dataframe() for all locations and filetypes"""
    if filetype == "parquet":
        data = str.encode("data")
    else:
        data = "data"
    with patch(type_method_map_fixture[FileType(filetype)]) as mocked_read, patch(
        "astro.files.base.smart_open.open", mock_open(read_data=data)
    ) as mocked_smart_open:

        path = locations["path"] + "." + filetype

        File(path).export_to_dataframe(normalize_config=None)
        mocked_smart_open.assert_called()
        kwargs = mocked_smart_open.call_args.kwargs
        args = mocked_smart_open.call_args.args
        if kwargs["transport_params"]:
            assert isinstance(
                kwargs["transport_params"]["client"], locations["instance"]
            )
        assert path == args[0]
        mocked_read.assert_called()


@pytest.mark.parametrize(
    "type_method_map_fixture", [{"method": "export_to_dataframe"}], indirect=True
)
@pytest.mark.parametrize(
    "locations",
    [
        {
            "path": "/tmp/sample",
            "instance": None,
        },
        {
            "path": "s3://tmp/sample",
            "instance": BaseClient,
        },
        {
            "path": "gs://tmp/sample",
            "instance": Client,
        },
    ],
    ids=["local", "s3", "gcs"],
)
@pytest.mark.parametrize("filetype", SUPPORTED_FILE_TYPES)
def test_read_with_explicit_valid_type(filetype, locations, type_method_map_fixture):
    """Test export_to_dataframe() for all locations and filetypes, where the file type is explicitly specified"""
    if filetype == "parquet":
        data = str.encode("data")
    else:
        data = "data"
    with patch(type_method_map_fixture[FileType(filetype)]) as mocked_read, patch(
        "astro.files.base.smart_open.open", mock_open(read_data=data)
    ) as mocked_smart_open:

        path = locations["path"]

        File(path=path, filetype=FileType(filetype)).export_to_dataframe(
            normalize_config=None
        )
        mocked_smart_open.assert_called()
        kwargs = mocked_smart_open.call_args.kwargs
        args = mocked_smart_open.call_args.args
        if kwargs["transport_params"]:
            assert isinstance(
                kwargs["transport_params"]["client"], locations["instance"]
            )
        assert path == args[0]
        mocked_read.assert_called()


@pytest.mark.parametrize(
    "invalid_path",
    [
        "/tmp/cklcdklscdksl.csv",
        "/tmp/cklcdklscdksl/*.csv",
    ],
)
def test_resolve_file_path_pattern_raise_exception(invalid_path, caplog):
    """resolve_file_path_pattern expected to fail with default 'if_file_doesnt_exist' exception strategy"""

    with pytest.raises(ValueError) as e:
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
            File("/tmp/file_a.csv", conn_id="test"),
            File("/tmp/file_a.csv", conn_id="test"),
            True,
        ),
        (
            File("/tmp/file_a.csv", conn_id="test", filetype=constants.FileType.CSV),
            File("/tmp/file_a.csv", conn_id="test", filetype=constants.FileType.CSV),
            True,
        ),
        (
            File("/tmp/file_a.csv", conn_id="test", filetype=constants.FileType.CSV),
            File("/tmp/file_a.csv", conn_id="test2", filetype=constants.FileType.JSON),
            False,
        ),
        (
            File("/tmp/file_a.csv", conn_id="test", filetype=constants.FileType.CSV),
            File("/tmp/file_b.csv", conn_id="test", filetype=constants.FileType.CSV),
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
            File(
                path=str(
                    pathlib.Path(
                        pathlib.Path(__file__).parent.parent, "data/sample.csv"
                    )
                )
            ),
            f"astro+file:{str(pathlib.Path(pathlib.Path(__file__).parent.parent, 'data/sample.csv'))}",
        ),
        (
            File(
                path="https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb_v2.csv"
            ),
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
    _convert_remote_file_to_byte_stream_called = files[
        "_convert_remote_file_to_byte_stream"
    ]
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
