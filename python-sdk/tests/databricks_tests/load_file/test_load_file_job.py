from unittest import mock
from unittest.mock import call

import pytest

from astro.constants import FileType
from astro.databricks.load_file.load_file_job import _find_file_type, _load_local_file_to_dbfs
from astro.files.base import File


def test_find_file_type():
    assert _find_file_type(File("foo.csv")) == "csv"
    assert _find_file_type(File("foo/bar/../../baz.csv")) == "csv"
    assert _find_file_type(File("foo.parquet")) == "parquet"
    assert _find_file_type(File("foo/bar/", filetype=FileType.CSV)) == "csv"
    with pytest.raises(ValueError):
        _find_file_type(File("foo/bar"))


@mock.patch("databricks_cli.sdk.api_client.ApiClient", autospec=True)
def test_load_local_file(mock_api_client):
    _load_local_file_to_dbfs(api_client=mock_api_client, input_file=File(__file__))
    calls = [
        call.perform_query("POST", "/dbfs/mkdirs", data={"path": "dbfs:/mnt/pyscripts/"}, headers=None),
        call.perform_query(
            "POST",
            "/dbfs/delete",
            data={"path": "dbfs:/mnt/pyscripts/test_load_file_job.py", "recursive": False},
            headers=None,
        ),
        call.perform_query(
            "POST",
            "/dbfs/put",
            data={"path": "dbfs:/mnt/pyscripts/test_load_file_job.py", "overwrite": True},
            headers={"Content-Type": None},
            files={"file": mock.ANY},
        ),
    ]
    mock_api_client.assert_has_calls(calls)
