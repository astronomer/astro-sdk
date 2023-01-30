import logging
import pathlib

import pandas as pd

import pytest
from airflow.decorators import task

from astro import sql as aql
from astro.constants import Database
from astro.files import File
from astro.table import BaseTable

from ..operators import utils as test_utils

CWD = pathlib.Path(__file__).parent
DATA_FILEPATH = pathlib.Path(CWD.parent.parent, "data/sample.csv")
DATA_FILEPATH_MSSQL = pathlib.Path(CWD.parent.parent, "data/sample_without_unicode.csv")


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

    @task
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

    expected_warning = "excessive amount of data being recorded to the Airflow metadata database"
    assert expected_warning in caplog.text


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.MSSQL, "file": File(path=str(DATA_FILEPATH_MSSQL))}],
    indirect=True,
    ids=["mssql"],
)
def test_run_raw_sql_without_limit_for_mssql(caplog, sample_dag, database_table_fixture):
    _, test_table = database_table_fixture
    caplog.set_level(logging.WARNING)

    @aql.run_raw_sql
    def raw_sql_query(input_table):
        return "SELECT * from {{input_table}}"

    @task
    def assert_num_rows(results):
        assert len(results) == 2
        assert results == [
            (1, "First"),
            (2, "Second"),
        ]

    with sample_dag:
        results = raw_sql_query(
            input_table=test_table,
            handler=lambda cursor: cursor.fetchall(),
        )
        assert_num_rows(results)

    test_utils.run_dag(sample_dag)

    expected_warning = "excessive amount of data being recorded to the Airflow metadata database"
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

    @task
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.MSSQL, "file": File(path=str(DATA_FILEPATH_MSSQL))}],
    indirect=True,
    ids=["mssql"],
)
def test_run_raw_sql_with_limit_for_mssql(sample_dag, database_table_fixture):
    _, test_table = database_table_fixture

    @aql.run_raw_sql
    def raw_sql_query(input_table):
        return "SELECT * from {{input_table}}"

    @task
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


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [{"database": Database.SQLITE, "file": File(path=str(DATA_FILEPATH))}],
    indirect=True,
    ids=["sqlite"],
)
def test_run_raw_sql_handle_multiple_tables(sample_dag, database_table_fixture):
    """
    Handle the case when we are passing multiple dataframe to run_raw_sql() operator
    and all the dataframes are converted to different tables.
    """
    _, test_table = database_table_fixture

    @aql.run_raw_sql(handler=lambda x: pd.DataFrame(x.fetchall(), columns=x.keys()))
    def raw_sql_query_1(input_table: BaseTable):
        return "SELECT * from {{input_table}}"

    @aql.run_raw_sql(handler=lambda x: pd.DataFrame(x.fetchall(), columns=x.keys()))
    def raw_sql_query_2(input_table: BaseTable):
        return "SELECT * from {{input_table}}"

    @aql.run_raw_sql(handler=lambda x: pd.DataFrame(x.fetchall(), columns=x.keys()), conn_id="sqlite_default")
    def raw_sql_query_3(table_1: BaseTable, table_2: BaseTable):
        assert table_1.name != table_2.name
        return "SELECT 1 + 1"

    with sample_dag:
        results_1 = raw_sql_query_1(input_table=test_table)
        results_2 = raw_sql_query_2(input_table=test_table)
        _ = raw_sql_query_3(table_1=results_1, table_2=results_2)
    test_utils.run_dag(sample_dag)


def test_run_raw_sql__results_format__pandas_dataframe(sample_dag, database_table_fixture):
    """run_raw_sql() command should return `pandas.DataFrame` when `results_format='pandas_dataframe' is passed"""
    _, test_table = database_table_fixture

    @aql.run_raw_sql(results_format="pandas_dataframe")
    def raw_sql_query(input_table):
        return "SELECT * from {{input_table}}"

    @task
    def assert_num_rows(result):
        assert isinstance(result, pd.DataFrame)
        assert result.equals(pd.read_csv(DATA_FILEPATH))
        assert result.shape == (3, 2)

    with sample_dag:
        results = raw_sql_query(input_table=test_table)
        assert_num_rows(results)

    test_utils.run_dag(sample_dag)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {"database": Database.SQLITE, "file": File(path=str(DATA_FILEPATH))},
        {"database": Database.SNOWFLAKE, "file": File(path=str(DATA_FILEPATH))},
        {"database": Database.POSTGRES, "file": File(path=str(DATA_FILEPATH))},
        {"database": Database.BIGQUERY, "file": File(path=str(DATA_FILEPATH))},
    ],
    indirect=True,
    ids=["sqlite", "snowflake", "postgres", "bigquery"],
)
def test_run_raw_sql__results_format__list(sample_dag, database_table_fixture):
    """run_raw_sql() command should return `List` when `results_format='list' is passed"""
    _, test_table = database_table_fixture

    @aql.run_raw_sql(results_format="list")
    def raw_sql_query(input_table):
        return "SELECT name from {{input_table}}"

    @task
    def assert_num_rows(result):
        assert isinstance(result, list)
        assert result[0] == ["First"]
        assert result[1] == ["Second"]
        assert result[2] == ["Third with unicode पांचाल"]
        assert len(result) == 3

    with sample_dag:
        results = raw_sql_query(input_table=test_table)
        assert_num_rows(results)
    test_utils.run_dag(sample_dag)


def test_run_raw_sql_handle_multiple_tables(sample_dag, database_table_fixture):
    """
    Handle the case when we are passing multiple dataframe to run_raw_sql() operator
    and all the dataframes are converted to different tables.
    """
    _, test_table = database_table_fixture

    @aql.run_raw_sql(handler=lambda x: pd.DataFrame(x.fetchall(), columns=x.keys()))
    def raw_sql_query_1(input_table: BaseTable):
        return "SELECT * from {{input_table}}"

    @aql.run_raw_sql(handler=lambda x: pd.DataFrame(x.fetchall(), columns=x.keys()))
    def raw_sql_query_2(input_table: BaseTable):
        return "SELECT * from {{input_table}}"

    @aql.run_raw_sql(handler=lambda x: pd.DataFrame(x.fetchall(), columns=x.keys()), conn_id="sqlite_default")
    def raw_sql_query_3(table_1: BaseTable, table_2: BaseTable):
        assert table_1.name != table_2.name
        return "SELECT 1 + 1"

    with sample_dag:
        results_1 = raw_sql_query_1(input_table=test_table)
        results_2 = raw_sql_query_2(input_table=test_table)
        _ = raw_sql_query_3(table_1=results_1, table_2=results_2)
    test_utils.run_dag(sample_dag)

