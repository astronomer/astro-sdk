import os
import pathlib
import shutil
import uuid

import pytest

from astro.constants import FileLocation
from astro.files.locations.base import Location
from astro.files.locations.local import Local

CWD = pathlib.Path(__file__).parent

LOCAL_FILEPATH = f"{CWD}/../../data/homes2.csv"
LOCAL_DIR = f"/tmp/{uuid.uuid4()}/"
LOCAL_DIR_FILE_1 = str(pathlib.Path(LOCAL_DIR, "file_1.txt"))
LOCAL_DIR_FILE_2 = str(pathlib.Path(LOCAL_DIR, "file_2.txt"))

sample_filepaths_per_location = [
    (FileLocation.LOCAL, LOCAL_FILEPATH),
]


@pytest.fixture()
def local_dir():
    """create temp dir"""
    os.mkdir(LOCAL_DIR)
    open(LOCAL_DIR_FILE_1, "a").close()
    open(LOCAL_DIR_FILE_2, "a").close()
    yield
    shutil.rmtree(LOCAL_DIR)


def describe_get_location_type():
    """test get_location_type()"""

    def with_supported_location():  # skipcq: PTC-W0065, PYL-W0612
        """With all the supported locations"""
        location = Location.get_location_type(LOCAL_FILEPATH)
        assert location == FileLocation.LOCAL

    def with_unsupported_location_raises_exception():  # skipcq: PYL-W0612, PTC-W0065
        """With all the unsupported locations, should raise a valueError exception"""
        unsupported_filepath = "invalid://some-file"
        with pytest.raises(ValueError) as exc_info:
            _ = Location.get_location_type(unsupported_filepath)
        expected_msg = "Unsupported scheme 'invalid' from path 'invalid://some-file'"
        assert exc_info.value.args[0] == expected_msg


def describe_get_transport_params():
    """test get_transport_params() method"""

    def with_local():  # skipcq: PYL-W0612, PTC-W0065
        """with local filepath"""
        location = Local(LOCAL_FILEPATH)
        assert location.get_transport_params() is None


def describe_get_paths():
    """test get_paths() method"""

    @pytest.mark.parametrize(
        "path", [LOCAL_DIR, LOCAL_DIR + "file_*"], ids=["without-prefix", "with-prefix"]
    )  # skipcq: PTC-W0065
    def with_local_dir(local_dir, path):  # skipcq: PYL-W0612, PTC-W0065
        """with local filepath"""
        location = Local(path)
        assert sorted(location.get_paths()) == [LOCAL_DIR_FILE_1, LOCAL_DIR_FILE_2]

    def with_unsupported_location(local_dir):  # skipcq: PYL-W0612, PTC-W0065
        """with unsupported filepath"""
        path = "invalid://some-file"
        with pytest.raises(ValueError) as exc_info:
            _ = Local(path)
        expected_msg = "Invalid path: 'invalid://some-file'"
        assert exc_info.value.args[0] == expected_msg
