import os
import uuid

import pytest
from astro.constants import FileLocation
from astro.files.locations import create_file_location, get_class_name

LOCAL_FILEPATH = f"/tmp/{uuid.uuid4()}"

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


def test_get_class_name_method_invalid_name():
    """Test invalid case of implicit naming dependency among the module name and class name for dynamic imports"""

    class Test:  # skipcq: PY-D0002
        __name__ = "test.some"

        class SomethingElseLocation:  # skipcq: PY-D0002
            pass

    with pytest.raises(ValueError) as exc_info:
        get_class_name(Test)

    expected_msg = "No expected class name found, please note that the class names should an expected formats."
    assert exc_info.value.args[0] == expected_msg
