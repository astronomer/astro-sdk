import os
import uuid

import pytest

from astro.constants import FileLocation
from astro.files.locations import create_file_location

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
