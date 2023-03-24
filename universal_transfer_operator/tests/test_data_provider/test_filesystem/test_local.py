import os
import pathlib
import shutil
import uuid

import pytest

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.filesystem.local import LocalDataProvider
from universal_transfer_operator.datasets.file.base import File

CWD = pathlib.Path(__file__).parent
DATA_DIR = str(CWD) + "/../../data/"

LOCAL_FILEPATH = f"{CWD}/../../data/homes2.csv"
LOCAL_DIR = f"/tmp/{uuid.uuid4()}/"
LOCAL_DIR_FILE_1 = str(pathlib.Path(LOCAL_DIR, "file_1.txt"))
LOCAL_DIR_FILE_2 = str(pathlib.Path(LOCAL_DIR, "file_2.txt"))

LOCAL_FILE_PATTERN_WITH_RESPONSE = [
    ("/abc/pqr/", True),
    ("/abc/pqr/abc.pqr", False),
    ("s3://abc/pqr", True),
    ("gs://abc/pqr", True),
    ("s3://abc/pqr/", True),
    ("gs://abc/pqr/", True),
    ("s3://abc/pqr/dummy.txt", False),
    ("gs://abc/pqr/dummy.txt", False),
    ("sftp://abc/pqr_*", True),
    ("sftp://abc/pqr/cat.txt", False),
]


@pytest.fixture()
def local_dir():
    """create temp dir"""
    os.mkdir(LOCAL_DIR)
    with open(LOCAL_DIR_FILE_1, "a") as f:
        f.write("Test String")

    with open(LOCAL_DIR_FILE_2, "a") as f:
        f.write("Test String")
    yield
    shutil.rmtree(LOCAL_DIR)


def test_size(local_dir):
    """Test get_size() of for local file."""
    dataset = File(path=LOCAL_DIR_FILE_1)
    assert LocalDataProvider(dataset=dataset, transfer_mode=TransferMode.NONNATIVE).size == 11


def test_get_paths_with_local_dir(local_dir):  # skipcq: PYL-W0612
    """with local filepath"""
    dataset = File(path=LOCAL_DIR)
    location = LocalDataProvider(dataset=dataset, transfer_mode=TransferMode.NONNATIVE)
    assert sorted(location.paths) == [LOCAL_DIR_FILE_1, LOCAL_DIR_FILE_2]


def test_file_pattern():
    """Test if the file path is a pattern(eg. s3://bucket/folder or /folder/sample_* etc."""
    for file_obj, response in LOCAL_FILE_PATTERN_WITH_RESPONSE:
        assert File(path=file_obj).is_pattern() is response
