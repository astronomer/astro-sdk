import logging
import pathlib

import pytest
from airflow.decorators import task
from astro import sql as aql
from astro.constants import Database
from astro.files import File
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent
DATA_FILEPATH = pathlib.Path(CWD.parent.parent, "data/sample.csv")


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.SQLITE, "file": File(path=str(DATA_FILEPATH))}],
    indirect=True,
    ids=["sqlite"],
)
def test_run_raw_sql_without_limit(caplog, sample_dag, database_table_fixture):
    _, test_table = database_table_fixture
    caplog.set_level(logging.WARNING)

    @aql.run_raw_sql
    def raw_sql_query(input_table):
        return "SELECT * from {{input_table}}"

    @aql.dataframe
    def assert_num_rows(results):
        assert len(results) == 3
        assert results == [
            (1, "First"),
            (2, "Second"),
            (3, "Third with unicode पांचाल"),
        ]

    with sample_dag:
        results = raw_sql_query(
            input_table=test_table,
            handler=lambda cursor: cursor.fetchall(),
        )
        assert_num_rows(results)

    test_utils.run_dag(sample_dag)

    expected_warning = (
        "excessive amount of data being recorded to the Airflow metadata database"
    )
    assert expected_warning in caplog.text


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.SQLITE, "file": File(path=str(DATA_FILEPATH))}],
    indirect=True,
    ids=["sqlite"],
)
def test_run_raw_sql_with_limit(sample_dag, database_table_fixture):
    _, test_table = database_table_fixture

    @aql.run_raw_sql
    def raw_sql_query(input_table):
        return "SELECT * from {{input_table}}"

    @aql.dataframe
    def assert_num_rows(results):
        assert len(results) == 1
        assert results == [(1, "First")]

    with sample_dag:
        results = raw_sql_query(
            input_table=test_table,
            response_size=1,
            handler=lambda cursor: cursor.fetchall(),
        )
        assert_num_rows(results)

    test_utils.run_dag(sample_dag)
