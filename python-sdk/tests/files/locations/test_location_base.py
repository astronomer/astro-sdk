import os
import uuid

import pytest

from astro.constants import FileLocation
from astro.files.locations import create_file_location, get_class_name
from astro.files.locations.local import LocalLocation

LOCAL_FILENAME = str(uuid.uuid4())
LOCAL_FILEPATH = f"/tmp/{LOCAL_FILENAME}"

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
def local_file():  # skipcq: PY-D0003
    open(LOCAL_FILEPATH, "a").close()
    yield
    os.remove(LOCAL_FILEPATH)


@pytest.mark.parametrize("filepath", sample_filepaths, ids=sample_filepaths_ids)
def test_validate_path_with_supported_filepaths(local_file, filepath):
    """Test is_valid_path with supported paths"""
    location = create_file_location(filepath)
    assert location.is_valid_path(filepath) is True


def test_get_class_name_method_valid_name():
    """Test valid case of implicit naming dependency among the module name and class name for dynamic imports"""

    class Test:  # skipcq: PY-D0002
        __name__ = "test.some"

        class TestLocation:  # skipcq: PY-D0002
            pass

    assert get_class_name(Test) == "TestLocation"


@pytest.mark.parametrize(
    "file_location,filepath,namespace",
    [
        (FileLocation.LOCAL, LOCAL_FILEPATH, LOCAL_FILENAME),
        (FileLocation.HTTP, "http://domain/some-file", "http://domain"),
        (FileLocation.HTTPS, "https://domain/some-file", "https://domain"),
        (FileLocation.S3, "s3://bucket/some-file", "s3://bucket"),
        (FileLocation.GS, "gs://bucket/some-file", "gs://bucket"),
    ],
)
def test_openlineage_file_dataset_namespace(file_location, filepath, namespace):
    """
    Test the open lineage dataset namespace as per
    https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
    """
    location = create_file_location(filepath)
    assert location.openlineage_dataset_namespace == namespace


@pytest.mark.parametrize(
    "file_location,filepath,dataset_name",
    [
        (FileLocation.LOCAL, LOCAL_FILEPATH, LOCAL_FILEPATH),
        (FileLocation.HTTP, "http://domain/some-file", "/some-file"),
        (FileLocation.HTTPS, "https://domain/some-file", "/some-file"),
        (FileLocation.S3, "s3://bucket/some-file", "/some-file"),
        (FileLocation.GS, "gs://bucket/some-file", "/some-file"),
    ],
)
def test_openlineage_file_dataset_name(file_location, filepath, dataset_name):
    """
    Test the open lineage dataset names as per
    https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
    """
    location = create_file_location(filepath)
    assert location.openlineage_dataset_name == dataset_name


def test_get_class_name_method_invalid_name():
    """Test invalid case of implicit naming dependency among the module name and class name for dynamic imports"""

    class Test:  # skipcq: PY-D0002
        __name__ = "test.some"

        class SomethingElseLocation:  # skipcq: PY-D0002
            pass

    with pytest.raises(ValueError) as exc_info:
        get_class_name(Test)

    expected_msg = (
        "No expected class name found, please note that the class names should an expected formats."
    )
    assert exc_info.value.args[0] == expected_msg


@pytest.mark.parametrize(
    "loc_1,loc_2,equality",
    [
        (LocalLocation("/tmp/file_a.csv"), LocalLocation("/tmp/file_a.csv"), True),
        (
            LocalLocation("/tmp/file_a.csv", conn_id="test"),
            LocalLocation("/tmp/file_a.csv", conn_id="test"),
            True,
        ),
        (
            LocalLocation("/tmp/file_a.csv", conn_id="test"),
            LocalLocation("/tmp/file_a.csv", conn_id="test"),
            True,
        ),
        (
            LocalLocation("/tmp/file_a.csv", conn_id="test"),
            LocalLocation("/tmp/file_a.csv", conn_id="test2"),
            False,
        ),
        (
            LocalLocation("/tmp/file_a.csv", conn_id="test"),
            LocalLocation("/tmp/file_b.csv", conn_id="test"),
            False,
        ),
    ],
)
def test_location_eq(loc_1, loc_2, equality):
    """Test that equality works"""
    if equality:
        assert loc_1 == loc_2
    else:
        assert loc_1 != loc_2


def test_location_hash():
    """Test that hashing works"""
    assert isinstance(hash(LocalLocation("/tmp/file_a.csv")), int)
