"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import pathlib

import pytest
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.settings import SCHEMA
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "sql_server",
    [
        pytest.param(
            "bigquery",
            marks=pytest.mark.xfail(
                reason="400 DELETE must have a WHERE clause at [1:1]"
            ),
        ),
        "snowflake",
        "postgres",
        "sqlite",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "path": str(CWD) + "/../data/homes2.csv",
            "load_table": True,
            "is_temp": False,
            "param": {
                "schema": SCHEMA,
                "table_name": test_utils.get_table_name("test_stats_check_1"),
            },
        },
    ],
    indirect=True,
)
def test_truncate(sql_server, test_table, sample_dag):
    sql_name, hook = sql_server
    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)
    assert df.count()[0] == 5
    with sample_dag:
        aql.truncate(
            table=test_table,
        )
    test_utils.run_dag(sample_dag)

    df = test_utils.get_dataframe_from_table(sql_name, test_table, hook)
    assert df.count()[0] == 0
