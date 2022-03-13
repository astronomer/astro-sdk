import pathlib

import pandas as pd
import pytest
from airflow.decorators import task, task_group
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session

from astro import sql as aql
from astro.dataframe import dataframe as adf
from astro.sql.operators.agnostic_boolean_check import Check
from astro.sql.table import Table, TempTable
from astro.utils.dependencies import PostgresHook, SnowflakeHook
from tests.operators import utils as test_utils

cwd = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        "bigquery",
        "sqlite",
    ],
    indirect=True,
)
def test_dataframe_transform(sql_server, sample_dag, tmp_table):
    print("test_dataframe_to_database")

    @adf
    def get_dataframe():
        return pd.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})

    @aql.transform
    def sample_pg(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    @adf
    def validate_dataframe(df: pd.DataFrame):
        assert df.equals(
            pd.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})
        )

    with sample_dag:
        my_df = get_dataframe(output_table=tmp_table)
        pg_df = sample_pg(my_df)
        validate_dataframe(pg_df)
    test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        "bigquery",
        "sqlite",
    ],
    indirect=True,
)
def test_transform(sql_server, sample_dag, tmp_table):
    @aql.transform
    def sample_function(input_table: Table):
        return "SELECT * FROM {{input_table}} LIMIT 10"

    @adf
    def validate_table(df: pd.DataFrame):
        assert len(df) == 10

    with sample_dag:
        homes_file = aql.load_file(
            path=str(cwd) + "/../data/homes.csv",
            output_table=tmp_table,
        )
        first_model = sample_function(
            input_table=homes_file,
        )
        inherit_model = sample_function(
            input_table=first_model,
        )
        validate_table(inherit_model)
    test_utils.run_dag(sample_dag)
