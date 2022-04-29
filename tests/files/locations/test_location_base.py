import os
import uuid

import pytest

from astro.constants import FileLocation
from astro.files.locations import location_factory

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


def describe_validate_path():  # skipcq: PY-D0003
    @pytest.mark.parametrize("filepath", sample_filepaths, ids=sample_filepaths_ids)
    def with_supported_filepaths(local_file, filepath):  # skipcq: PTC-W0065, PY-D0003
        location = location_factory(filepath)
        assert location.is_valid_path(filepath) is True

    def with_unsupported_path_raises_exception():  # skipcq: PYL-W0612, PTC-W0065, PY-D0003
        nonexistent_file = "/tmp/nonexistent-file"
        with pytest.raises(ValueError) as exc_info:
            _ = location_factory(nonexistent_file)
        expected_msg = "Invalid path: '/tmp/nonexistent-file'"
        assert exc_info.value.args[0] == expected_msg
