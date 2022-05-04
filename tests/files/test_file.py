import pathlib
from unittest.mock import patch

import pandas as pd
import pytest
from botocore.client import BaseClient
from google.cloud.storage import Client

from astro.constants import (
    SUPPORTED_FILE_LOCATIONS,
    SUPPORTED_FILE_TYPES,
    FileLocation,
    FileType,
)
from astro.files import File, get_files

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
    File(files["path"]).exists()
    mocked_smart_open.assert_called()
    kwargs = mocked_smart_open.call_args.kwargs
    args = mocked_smart_open.call_args.args
    if kwargs["transport_params"]:
        assert isinstance(kwargs["transport_params"]["client"], files["instance"])
    assert files["path"] == args[0]


@pytest.mark.parametrize(
    "type_method_map_fixture", [{"method": "write_from_dataframe"}], indirect=True
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
def test_write(mocked_smart_open, filetype, locations, type_method_map_fixture):
    data = {"id": [1, 2, 3], "name": ["First", "Second", "Third with unicode पांचाल"]}
    df = pd.DataFrame(data=data)
    with patch(type_method_map_fixture[FileType(filetype)]) as mocked_write:

        path = locations["path"] + "." + filetype

        File(path).write_from_dataframe(df=df)
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
    "type_method_map_fixture", [{"method": "read_to_dataframe"}], indirect=True
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
def test_read(mocked_smart_open, filetype, locations, type_method_map_fixture):
    with patch(type_method_map_fixture[FileType(filetype)]) as mocked_read:

        path = locations["path"] + "." + filetype

        File(path).read_to_dataframe(normalize_config=None)
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
    "type_method_map_fixture", [{"method": "read_to_dataframe"}], indirect=True
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
def test_read_with_explicit_valid_type(
    mocked_smart_open, filetype, locations, type_method_map_fixture
):
    with patch(type_method_map_fixture[FileType(filetype)]) as mocked_read:

        path = locations["path"]

        File(path=path, filetype=FileType(filetype)).read_to_dataframe(
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
    "locations_method_map_fixture", [{"method": "paths"}], indirect=True
)
@pytest.mark.parametrize("file_location", SUPPORTED_FILE_LOCATIONS)
@pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
def test_get_files(file_type, file_location, locations_method_map_fixture):
    path = f"{file_location}://tmp/sample.{file_type}"
    if file_location == FileLocation.LOCAL.value:
        path = f"/tmp/sample.{file_type}"

    patch_module = locations_method_map_fixture[FileLocation(file_location)]

    with patch(patch_module):
        files = get_files(path)
        for file in files:
            assert file.location.location_type.value == file_location
            assert file.type.name.value == file_type
