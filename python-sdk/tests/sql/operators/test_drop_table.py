"""Tests to cover the drop table function"""

import pathlib

import pandas
import pytest
from airflow.decorators import task

import astro.sql as aql
from astro.constants import Database
from astro.files import File
from astro.table import Table
from tests.sql.operators import utils as test_utils

CWD = pathlib.Path(__file__).parent
DEFAULT_FILEPATH = str(pathlib.Path(CWD.parent.parent, "data/sample.csv").absolute())


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
        {
            "database": Database.REDSHIFT,
            "file": File(DEFAULT_FILEPATH),
        },
    ],
    indirect=True,
    ids=["sqlite", "postgres", "bigquery", "snowflake", "redshift"],
)
def test_drop_table_with_table_metadata(database_table_fixture, sample_dag):
    """Test drop table operator for all databases."""
    database, test_table = database_table_fixture
    assert database.table_exists(test_table)
    tmp_table = test_table.create_similar_table()

    @aql.dataframe
    def do_nothing(df: pandas.DataFrame):
        return df

    @task
    def validate_table_exists(table: Table):
        assert database.table_exists(table)
        assert table.name == tmp_table.name
        return table

    with sample_dag:
        transformed_operator = do_nothing(test_table, output_table=tmp_table)
        validated_table = validate_table_exists(transformed_operator)
        aql.drop_table(
            table=validated_table,
        )
    test_utils.run_dag(sample_dag)

    assert not database.table_exists(tmp_table)


@pytest.mark.integration
@pytest.mark.parametrize(
    "database_table_fixture",
    [
        {
            "database": Database.POSTGRES,
            "table": Table(conn_id="postgres_conn"),
            "file": File(DEFAULT_FILEPATH),
        },
        {
            "database": Database.REDSHIFT,
            "table": Table(conn_id="redshift_conn"),
            "file": File(DEFAULT_FILEPATH),
        },
    ],
    indirect=True,
    ids=["postgres", "redshift"],
)
def test_drop_table_without_table_metadata(database_table_fixture, sample_dag):
    """Test drop table operator for all databases."""
    database, test_table = database_table_fixture
    assert database.table_exists(test_table)

    with sample_dag:
        aql.drop_table(
            table=test_table,
        )
    test_utils.run_dag(sample_dag)

    assert not database.table_exists(test_table)
