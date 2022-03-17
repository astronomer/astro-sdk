import os
import pathlib
import shutil
import uuid

import pytest

from astro.constants import FileLocation
from astro.utils.path import (
    get_location,
    get_paths,
    get_transport_params,
    is_local,
    validate_path,
)

LOCAL_FILEPATH = f"/tmp/{uuid.uuid4()}"
LOCAL_DIR = f"/tmp/{uuid.uuid4()}/"
LOCAL_DIR_FILE_1 = str(pathlib.Path(LOCAL_DIR, "file_1.txt"))
LOCAL_DIR_FILE_2 = str(pathlib.Path(LOCAL_DIR, "file_2.txt"))

sample_filepaths_per_location = [
    (FileLocation.LOCAL, LOCAL_FILEPATH),
    (FileLocation.HTTP, "http://domain/some-file"),
    (FileLocation.HTTPS, "https://domain/some-file"),
    (FileLocation.S3, "s3://bucket/some-file"),
    (FileLocation.GS, "gs://bucket/some-file"),
]
sample_filepaths = [items[1] for items in sample_filepaths_per_location]
sample_filepaths_ids = [items[0].value for items in sample_filepaths_per_location]


@pytest.fixture()
def local_file():
    open(LOCAL_FILEPATH, "a").close()
    yield
    os.remove(LOCAL_FILEPATH)


@pytest.fixture()
def local_dir():
    os.mkdir(LOCAL_DIR)
    open(LOCAL_DIR_FILE_1, "a").close()
    open(LOCAL_DIR_FILE_2, "a").close()
    yield
    shutil.rmtree(LOCAL_DIR)


def describe_get_location():
    @pytest.mark.parametrize(
        "expected_location,filepath",
        sample_filepaths_per_location,
        ids=sample_filepaths_ids,
    )
    def with_supported_location(expected_location, filepath):
        location = get_location(filepath)
        assert location == expected_location

    def with_unsupported_location_raises_exception():
        unsupported_filepath = "invalid://some-file"
        with pytest.raises(ValueError) as exc_info:
            get_location(unsupported_filepath)
        expected_msg = "Unsupported scheme 'invalid' from path 'invalid://some-file'"
        assert exc_info.value.args[0] == expected_msg


def describe_validate_path():
    @pytest.mark.parametrize("filepath", sample_filepaths, ids=sample_filepaths_ids)
    def with_supported_filepaths(local_file, filepath):
        assert validate_path(filepath) is None

    def with_unsupported_path_raises_exception():
        nonexistent_file = "/tmp/nonexistent-file"
        with pytest.raises(ValueError) as exc_info:
            validate_path(nonexistent_file)
        expected_msg = "Invalid path: '/tmp/nonexistent-file'"
        assert exc_info.value.args[0] == expected_msg


def describe_get_transport_params():
    def with_gcs():
        path = "gs://bucket/some-file"
        credentials = get_transport_params(path, None)
        assert "google.cloud.storage.client.Client" in str(
            credentials["client"].__class__
        )

    def with_s3():
        path = "s3://bucket/some-file"
        credentials = get_transport_params(path, None)
        assert "botocore.client.S3" in str(credentials["client"].__class__)

    def with_local():
        path = "/tmp/file"
        credentials = get_transport_params(path, None)
        assert credentials is None

    def with_api():
        path = "http://domain/file"
        credentials = get_transport_params(path, None)
        assert credentials is None

        path = "https://domain/file"
        credentials = get_transport_params(path, None)
        assert credentials is None


def describe_get_paths():
    def with_api():
        path = "http://domain/some-file.txt"
        assert get_paths(path) == [path]

        path = "https://domain/some-file.txt"
        assert get_paths(path) == [path]

    def with_local_dir(local_dir):
        path = LOCAL_DIR
        assert get_paths(path) == [LOCAL_DIR_FILE_1, LOCAL_DIR_FILE_2]

    def with_local_prefix(local_dir):
        path = LOCAL_DIR + "file_*"
        assert get_paths(path) == [LOCAL_DIR_FILE_1, LOCAL_DIR_FILE_2]

    @pytest.mark.integration
    @pytest.mark.parametrize(
        "remote_file",
        [{"name": "google", "count": 2}, {"name": "amazon", "count": 2}],
        ids=["google", "amazon"],
        indirect=True,
    )
    def with_remote_object_store_prefix(remote_file):
        _, objects_uris = remote_file
        objects_prefix = objects_uris[0][:-5]
        assert len(objects_uris) == 2
        assert get_paths(objects_prefix) == objects_uris

    def with_unsupported_location(local_dir):
        path = "invalid://some-file"
        with pytest.raises(ValueError) as exc_info:
            get_paths(path)
        expected_msg = "Unsupported scheme 'invalid' from path 'invalid://some-file'"
        assert exc_info.value.args[0] == expected_msg


def describe_is_local():
    def with_local_file():
        path = "/tmp/something"
        assert is_local(path)

    def with_remote_file():
        path = "gs://tmp/something"
        assert not is_local(path)
