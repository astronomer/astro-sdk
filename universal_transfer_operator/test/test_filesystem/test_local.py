import os
import pathlib
import shutil
import uuid
import pytest

import pandas as pd
from utils.test_utils import create_unique_str

from universal_transfer_operator.data_providers import create_dataprovider
from universal_transfer_operator.data_providers.filesystem.base import FileStream
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.data_providers.filesystem.local import LocalDataProvider
CWD = pathlib.Path(__file__).parent
DATA_DIR = str(CWD) + "/../../data/"

LOCAL_FILEPATH = f"{CWD}/../../data/homes2.csv"
LOCAL_DIR = f"/tmp/{uuid.uuid4()}/"
LOCAL_DIR_FILE_1 = str(pathlib.Path(LOCAL_DIR, "file_1.txt"))
LOCAL_DIR_FILE_2 = str(pathlib.Path(LOCAL_DIR, "file_2.txt"))


@pytest.fixture()
def local_dir():
    """create temp dir"""
    os.mkdir(LOCAL_DIR)
    open(LOCAL_DIR_FILE_1, "a").close()
    open(LOCAL_DIR_FILE_2, "a").close()
    yield
    shutil.rmtree(LOCAL_DIR)

def test_size():
    """Test get_size() of for local file."""
    dataset = File(path=LOCAL_DIR_FILE_1)
    assert LocalDataProvider(dataset).size == 65

def test_get_paths_with_local_dir(local_dir):  # skipcq: PYL-W0612
    """with local filepath"""
    dataset = File(path=LOCAL_DIR_FILE_1)
    location = LocalDataProvider(dataset)
    assert sorted(location.paths) == [LOCAL_DIR_FILE_1]