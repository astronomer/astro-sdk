import os
import pathlib
import shutil
import uuid

import pytest

from astro.constants import FileLocation
from astro.files.locations import location_factory, location_type

CWD = pathlib.Path(__file__).parent

LOCAL_FILEPATH = f"{CWD}/../../../data/homes2.csv"
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
        location = location_type(filepath)
        assert location == expected_location

    def with_unsupported_location_raises_exception():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        unsupported_filepath = "invalid://some-file"
        with pytest.raises(ValueError) as exc_info:
            _ = location_type(unsupported_filepath)
        expected_msg = "Unsupported scheme 'invalid' from path 'invalid://some-file'"
        assert exc_info.value.args[0] == expected_msg


def describe_validate_path():
    @pytest.mark.parametrize("filepath", sample_filepaths, ids=sample_filepaths_ids)
    def with_supported_filepaths(filepath):
        location = location_factory(filepath)
        assert location.is_valid_path(filepath) is True

    def with_unsupported_path_raises_exception():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        nonexistent_file = "/tmp/nonexistent-file"
        with pytest.raises(ValueError) as exc_info:
            _ = location_factory(nonexistent_file)
        expected_msg = "Invalid path: '/tmp/nonexistent-file'"
        assert exc_info.value.args[0] == expected_msg


def describe_get_transport_params():
    def with_gcs():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        path = "gs://bucket/some-file"
        location = location_factory(path)
        credentials = location.get_transport_params()
        assert "google.cloud.storage.client.Client" in str(
            credentials["client"].__class__
        )

    def with_s3():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        path = "s3://bucket/some-file"
        location = location_factory(path)
        credentials = location.get_transport_params()
        assert "botocore.client.S3" in str(credentials["client"].__class__)

    def with_local():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        location = location_factory(LOCAL_FILEPATH)
        credentials = location.get_transport_params()
        assert credentials is None

    def with_api():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        path = "http://domain/file"
        location = location_factory(path)
        credentials = location.get_transport_params()
        assert credentials is None

        path = "https://domain/file"
        location = location_factory(path)
        credentials = location.get_transport_params()
        assert credentials is None


def describe_get_paths():
    def with_api():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        path = "http://domain/some-file.txt"
        location = location_factory(path)
        assert location.get_paths() == [path]

        path = "https://domain/some-file.txt"
        location = location_factory(path)
        assert location.get_paths() == [path]

    def with_local_dir(local_dir):  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        path = LOCAL_DIR
        location = location_factory(path)
        assert sorted(location.get_paths()) == [LOCAL_DIR_FILE_1, LOCAL_DIR_FILE_2]

    def with_local_prefix(local_dir):  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        path = LOCAL_DIR + "file_*"
        location = location_factory(path)
        assert sorted(location.get_paths()) == [LOCAL_DIR_FILE_1, LOCAL_DIR_FILE_2]

    def with_unsupported_location(local_dir):  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        path = "invalid://some-file"
        with pytest.raises(ValueError) as exc_info:
            _ = location_factory(path)
        expected_msg = "Unsupported scheme 'invalid' from path 'invalid://some-file'"
        assert exc_info.value.args[0] == expected_msg

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
        location = location_factory(objects_prefix)
        assert len(objects_uris) == 2
        assert sorted(location.get_paths()) == sorted(objects_uris)
