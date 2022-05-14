"""
Unittest module to test Operators.
Requires the unittest, pytest, and requests-mock Python libraries.
"""

import logging
import pathlib

import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.constants import SUPPORTED_DATABASES
from astro.sql.operators.agnostic_boolean_check import Check
from astro.sql.tables import Table
from tests.operators.utils import run_dag

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


@pytest.mark.integration
@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "is_temp": True,
            "path": str(CWD) + "/../data/homes_append.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_happyflow_success(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        temp_table = get_table(test_table)
        aql.boolean_check(
            table=temp_table,
            checks=[Check("test_1", "rooms > 3")],
            max_rows_returned=10,
        )
    run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize("sql_server", SUPPORTED_DATABASES, indirect=True)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "is_temp": True,
            "path": str(CWD) + "/../data/homes_append.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_happyflow_fail(sample_dag, test_table, caplog):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            temp_table = get_table(test_table)
            aql.boolean_check(
                table=temp_table,
                checks=[
                    Check("test_1", "rooms > 7"),
                    Check("test_2", "beds >= 3"),
                ],
                max_rows_returned=10,
            )
        run_dag(sample_dag)
    expected_error = "Some of the check(s) have failed"
    assert expected_error in caplog.text


@pytest.mark.integration
@pytest.mark.parametrize(
    "sql_server",
    [
        "postgres",
        pytest.param(
            "bigquery",
            marks=pytest.mark.xfail(
                reason="bigquery don't expect table name before cols."
            ),
        ),
        pytest.param(
            "snowflake",
            marks=pytest.mark.xfail(
                reason="Binding data in type (table) is not supported."
            ),
        ),
        "sqlite",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        {
            "is_temp": True,
            "path": str(CWD) + "/../data/homes_append.csv",
            "load_table": True,
        }
    ],
    indirect=True,
    ids=["table"],
)
def test_happyflow_success_with_templated_query(sample_dag, test_table):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        temp_table = get_table(test_table)
        aql.boolean_check(
            table=temp_table,
            checks=[Check("test_1", "{{table}}.rooms > 3")],
            max_rows_returned=10,
        )
    run_dag(sample_dag)
