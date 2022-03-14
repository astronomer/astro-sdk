"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
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


@pytest.mark.parametrize("sql_server", ["snowflake"])
def test_roles_failing(sql_server, sample_dag):
    table = snowflake_table(sample_dag.dag_id + "_role_failing", role="foo")
    with pytest.raises(Exception):
        run_role_query(sample_dag, table, role="foo")


@pytest.mark.parametrize("sql_server", ["snowflake"])
def test_roles_passing(sql_server, sample_dag):
    table = snowflake_table(
        sample_dag.dag_id + "_role_passing", role=os.getenv("SNOWFLAKE_ROLE")
    )
    run_role_query(sample_dag, table, role=os.getenv("SNOWFLAKE_ROLE"))
