"""Tests specific to the Sqlite Database implementation."""

import pathlib

import pytest

from astro.constants import Database
from astro.files import File

DEFAULT_CONN_ID = "sqlite_default"
CUSTOM_CONN_ID = "sqlite_conn"
SUPPORTED_CONN_IDS = [DEFAULT_CONN_ID, CUSTOM_CONN_ID]
CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SQLITE},
    ],
    indirect=True,
    ids=["sqlite"],
)
def test_export_table_to_file_file_already_exists_raises_exception(
    database_table_fixture,
):
    """Raise exception if trying to export to file that exists"""
    database, source_table = database_table_fixture
    filepath = pathlib.Path(CWD.parent, "data/sample.csv")
    with pytest.raises(FileExistsError) as exception_info:
        database.export_table_to_file(source_table, File(str(filepath)))
    err_msg = exception_info.value.args[0]
    assert err_msg.endswith(f"The file {filepath} already exists.")
