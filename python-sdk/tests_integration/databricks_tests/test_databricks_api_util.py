import pathlib

import pytest

from astro.constants import Database
from astro.databricks.api_utils import delete_secret_scope

CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.DELTA,
        }
    ],
    indirect=True,
    ids=["delta"],
)
def test_delete_scope_nonexistent(database_table_fixture):
    db, _ = database_table_fixture
    delete_secret_scope("non-existent-scope", api_client=db.api_client)
