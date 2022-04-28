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


def test_get_size():
    """Test get_size() of for local file."""
    assert File(sample_file).get_size() == 65


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
def test_write(mocked_smart_open, filetype, locations):
    data = {"id": [1, 2, 3], "name": ["First", "Second", "Third with unicode पांचाल"]}
    df = pd.DataFrame(data=data)
    filetype_to_class = {
        FileType.JSON: "astro.files.type.Json.write",
        FileType.CSV: "astro.files.type.CSV.write",
        FileType.NDJSON: "astro.files.type.NdJson.write",
        FileType.PARQUET: "astro.files.type.Parquet.write",
    }
    mocked_write = patch(filetype_to_class[FileType(filetype)]).start()

    path = locations["path"] + "." + filetype

    File(path).write(df=df)
    mocked_smart_open.assert_called()
    kwargs = mocked_smart_open.call_args.kwargs
    args = mocked_smart_open.call_args.args
    if kwargs["transport_params"]:
        assert isinstance(kwargs["transport_params"]["client"], locations["instance"])
    assert path == args[0]
    mocked_write.assert_called()

    mocked_write.stop()


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
def test_read(mocked_smart_open, filetype, locations):
    filetype_to_class = {
        FileType.JSON: "astro.files.type.Json.read",
        FileType.CSV: "astro.files.type.CSV.read",
        FileType.NDJSON: "astro.files.type.NdJson.read",
        FileType.PARQUET: "astro.files.type.Parquet.read",
    }
    mocked_read = patch(filetype_to_class[FileType(filetype)]).start()

    path = locations["path"] + "." + filetype

    File(path).read(normalize_config=None)
    mocked_smart_open.assert_called()
    kwargs = mocked_smart_open.call_args.kwargs
    args = mocked_smart_open.call_args.args
    if kwargs["transport_params"]:
        assert isinstance(kwargs["transport_params"]["client"], locations["instance"])
    assert path == args[0]
    mocked_read.assert_called()

    mocked_read.stop()


@pytest.mark.parametrize("file_location", SUPPORTED_FILE_LOCATIONS)
@pytest.mark.parametrize("file_type", SUPPORTED_FILE_TYPES)
def test_get_files(file_type, file_location):
    filetype_to_class = {
        FileLocation.LOCAL: "astro.files.locations.local.LocalLocation.get_paths",
        FileLocation.HTTP: "astro.files.locations.http.HttpLocation.get_paths",
        FileLocation.HTTPS: "astro.files.locations.http.HttpLocation.get_paths",
        FileLocation.GS: "astro.files.locations.gcs.GCSLocation.get_paths",
        FileLocation.S3: "astro.files.locations.s3.S3Location.get_paths",
    }

    path = f"{file_location}://tmp/sample.{file_type}"
    if file_location == FileLocation.LOCAL.value:
        path = f"/tmp/sample.{file_type}"

    patch_module = filetype_to_class[FileLocation(file_location)]

    mocket_get_path = patch(patch_module).start()

    files = get_files(path)
    for file in files:
        assert file.location.location_type.value == file_location
        assert file.type.name.value == file_type

    mocket_get_path.stop()
