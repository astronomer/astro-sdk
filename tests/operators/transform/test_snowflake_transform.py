"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_snowflake_decorator.TestSnowflakeOperator

"""

import logging
import os
import pathlib

import pandas as pd
import pytest
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

# Import Operator
from astro import sql as aql
from astro.dataframe import dataframe as adf
from astro.sql.table import Table, TempTable
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent


def drop_table(table_name, snowflake_conn):
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


def get_snowflake_hook():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn",
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )
    return hook


def snowflake_table(table_name, role):
    hook = get_snowflake_hook()

    drop_table(
        snowflake_conn=hook.get_conn(),
        table_name=table_name,
    )
    return Table(
        table_name,
        conn_id="snowflake_conn",
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        role=role,
    )


def run_role_query(dag, table, role):
    @aql.transform
    def sample_snow(input_table: Table):
        return "SELECT * FROM {{input_table}} LIMIT 10"

    @adf
    def validate_table(df: pd.DataFrame):
        assert len(df) == 10

    with dag:
        loaded_table = aql.load_file(
            path=str(CWD) + "/../../data/homes.csv",
            output_table=table,
        )

        f = sample_snow(
            input_table=loaded_table,
            output_table=TempTable(
                conn_id="snowflake_conn",
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                role=role,
            ),
        )
        x = sample_snow(
            input_table=f,
        )
        validate_table(x)
    test_utils.run_dag(dag)


@pytest.mark.parametrize("sql_server", ["snowflake"], indirect=True)
def test_roles_failing(sql_server, sample_dag, test_table):
    test_table.role = "foo"
    with pytest.raises(Exception):
        run_role_query(sample_dag, test_table, role="foo")


@pytest.mark.parametrize("sql_server", ["snowflake"], indirect=True)
def test_roles_passing(sql_server, sample_dag, test_table):
    test_table.role = os.getenv("SNOWFLAKE_ROLE")
    run_role_query(sample_dag, test_table, role=os.getenv("SNOWFLAKE_ROLE"))


def run_simple_transform(dag, table, role):
    """Reproduces a live issue #319"""

    @aql.transform(conn_id="snowflake_conn")
    def select_without_input_table():
        """A transform without input_table"""
        return """
            select catalog_name as database,
                schema_name,
                schema_owner,
                created,
                last_altered
            from information_schema.schemata
            order by schema_name;
        """

    with dag:
        select_without_input_table(output_table=table)

    test_utils.run_dag(dag)


@pytest.mark.parametrize("sql_server", ["snowflake"], indirect=True)
def test_transform_without_input_table(sql_server, sample_dag, test_table):
    """Reproduces issue #319"""
    test_table.role = os.getenv("SNOWFLAKE_ROLE")
    run_simple_transform(sample_dag, test_table, role=os.getenv("SNOWFLAKE_ROLE"))
