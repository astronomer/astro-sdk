from unittest.mock import patch

from astro.constants import FileType
from astro.files import File


@patch("astro.files.locations.base.BaseFileLocation.validate_conn", return_value=None)
def test_str(validate_conn):
    file = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file.__str__() == "astro"


@patch("astro.files.locations.base.BaseFileLocation.validate_conn", return_value=None)
def test_repr(validate_conn):
    file = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file.__repr__() == (
        "File(path='astro', conn_id='local', filetype=<FileType.CSV: 'csv'>, "
        "normalize_config=None, is_dataframe=False, is_bytes=False, "
        "uri='astro+file://local@/astro?filetype=csv', extra={})"
    )


@patch("astro.files.locations.base.BaseFileLocation.validate_conn", return_value=None)
def test_eq(validate_conn):
    file1 = File(path="astro", conn_id="local", filetype=FileType.CSV)
    file2 = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file1.__eq__(file2) is True


@patch("astro.files.locations.base.BaseFileLocation.validate_conn", return_value=None)
def test_hash(validate_conn):
    file = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file.__hash__() is not None
