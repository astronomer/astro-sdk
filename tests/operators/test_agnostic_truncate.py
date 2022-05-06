"""Tests to cover the truncate decorator"""

import logging
import pathlib

import pytest
from airflow.utils import timezone

import astro.sql as aql
from astro.constants import Database
from astro.files import File
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
CWD = pathlib.Path(__file__).parent
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
DEFAULT_FILEPATH = str(pathlib.Path(CWD.parent, "data/sample.csv").absolute())


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.SQLITE,
            "file": File(DEFAULT_FILEPATH),
        },
        {
            "database": Database.POSTGRES,
            "file": File(DEFAULT_FILEPATH),
        },
        {
            "database": Database.BIGQUERY,
            "file": File(DEFAULT_FILEPATH),
        },
        {
            "database": Database.SNOWFLAKE,
            "file": File(DEFAULT_FILEPATH),
        },
    ],
    indirect=True,
    ids=["sqlite", "postgres", "bigquery", "snowflake"],
)
def test_truncate(database_table_fixture, sample_dag):
    """Test truncate operator for all databases."""
    database, test_table = database_table_fixture
    assert database.table_exists(test_table)

    with sample_dag:
        aql.truncate(
            table=test_table,
        )
    test_utils.run_dag(sample_dag)

    assert not database.table_exists(test_table)
