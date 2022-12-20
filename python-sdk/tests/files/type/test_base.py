from __future__ import annotations

from astro.constants import FileType
from astro.files import File


def test_str():
    file = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file.__str__() == "astro"


def test_repr():
    file = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file.__repr__() == (
        "File(path='astro', conn_id='local', filetype=<FileType.CSV: 'csv'>, "
        "normalize_config=None, is_dataframe=False, is_bytes=False, "
        "uri='astro+file://local@/astro?filetype=csv', extra={})"
    )


def test_eq():
    file1 = File(path="astro", conn_id="local", filetype=FileType.CSV)
    file2 = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file1.__eq__(file2) is True


def test_hash():
    file = File(path="astro", conn_id="local", filetype=FileType.CSV)
    assert file.__hash__() is not None
