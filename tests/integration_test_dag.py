import pathlib

import pandas as pd
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session

from astro import sql as aql
from astro.dataframe import dataframe as adf
from astro.sql.table import Table, TempTable
from astro.utils.dependencies import PostgresHook, SnowflakeHook
from tests.operators import utils as test_utils

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

CWD = pathlib.Path(__file__).parent
import pytest


@pytest.fixture
def sql_server(request):
    sql_name = request.param
    hook_parameters = test_utils.SQL_SERVER_HOOK_PARAMETERS.get(sql_name)
    hook_class = test_utils.SQL_SERVER_HOOK_CLASS.get(sql_name)
    if hook_parameters is None or hook_class is None:
        raise ValueError(f"Unsupported SQL server {sql_name}")
    hook = hook_class(**hook_parameters)
    schema = hook_parameters.get("schema", test_utils.DEFAULT_SCHEMA)
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")
    yield (sql_name, hook)
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


@pytest.fixture
def tmp_table(sql_server):
    sql_name, hook = sql_server

    if isinstance(hook, SnowflakeHook):
        return TempTable(
            conn_id=hook.snowflake_conn_id,
            database=hook.database,
            warehouse=hook.warehouse,
        )
    elif isinstance(hook, PostgresHook):
        return TempTable(conn_id=hook.postgres_conn_id, database=hook.schema)
    elif isinstance(hook, SqliteHook):
        return TempTable(conn_id=hook.sqlite_conn_id, database="sqlite")
    # elif isinstance(hook, BigQueryHook):
    #     return TempTable(conn_id=hook.gcp_conn_id, database=)


@aql.transform
def do_a_thing(input_table: Table):
    return "SELECT * FROM {{input_table}}"


@adf
def do_a_dataframe_thing(df: pd.DataFrame):
    return df


@pytest.mark.parametrize(
    "sql_server", ["snowflake", "postgres", "bigquery", "sqlite"], indirect=True
)
def test_full_dag(sql_server, sample_dag, tmp_table):
    with sample_dag:
        output_table = tmp_table
        loaded_table = aql.load_file(
            str(CWD) + "/data/homes.csv", output_table=output_table
        )
        tranformed_table = do_a_thing(loaded_table)
        dataframe_from_table = do_a_dataframe_thing(
            tranformed_table, output_table=output_table
        )
        validated_table = aql.aggregate_check(
            dataframe_from_table,
            check="select count(*) FROM {{table}}",
            equal_to=47,
        )
        aql.save_file(
            input=validated_table.output,
            output_file_path="/tmp/out.csv",
            overwrite=True,
        )
    test_utils.run_dag(sample_dag)
