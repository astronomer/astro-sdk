import pathlib

import pandas as pd
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


@adf
def count_dataframes(df1: pd.DataFrame, df2: pd.DataFrame):
    return len(df1) + len(df2)


@task
def compare(a, b):
    assert a == b


@task_group
def run_validation(input_table: Table):
    agg_validated_table = aql.aggregate_check(
        input_table,
        check="select count(*) FROM {{table}}",
        equal_to=47,
    )
    aql.save_file(
        input=agg_validated_table.output,
        output_file_path="/tmp/out_agg.csv",
        overwrite=True,
    )

    # TODO add boolean and stats checks here


@task_group
def run_dataframe_funcs(input_table: Table):
    table_counts = count_dataframes(df1=input_table, df2=input_table)

    df1 = do_a_dataframe_thing(input_table)
    df2 = do_a_dataframe_thing(input_table)
    df_counts = count_dataframes(df1, df2)
    compare(table_counts, df_counts)


@aql.run_raw_sql
def add_constraint(table: Table):
    if table.conn_type == "sqlite":
        return "CREATE UNIQUE INDEX unique_index ON {{table}}(list,sell)"
    return "ALTER TABLE {{table}} ADD CONSTRAINT airflow UNIQUE (list,sell)"


@task_group
def run_append(output_specs: TempTable):
    load_main = aql.load_file(
        path=str(CWD) + "/data/homes_main.csv",
        output_table=output_specs,
    )
    load_append = aql.load_file(
        path=str(CWD) + "/data/homes_append.csv",
        output_table=output_specs,
    )

    app = aql.append(
        columns=["sell", "living"],
        main_table=load_main,
        append_table=load_append,
    )


@task_group
def run_merge(output_specs: TempTable):
    main_table = aql.load_file(
        path=str(CWD) + "/data/homes_merge_1.csv",
        output_table=output_specs,
    )
    merge_table = aql.load_file(
        path=str(CWD) + "/data/homes_merge_2.csv",
        output_table=output_specs,
    )

    con1 = add_constraint(main_table)

    merged_table = aql.merge(
        target_table=main_table,
        merge_table=merge_table,
        merge_keys=["list", "sell"],
        target_columns=["list", "sell"],
        merge_columns=["list", "sell"],
        conflict_strategy="ignore",
    )
    con1 >> merged_table


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        pytest.param("bigquery", marks=pytest.mark.xfail(reason="some bug")),
        "sqlite",
    ],
    indirect=True,
)
def test_full_dag(sql_server, sample_dag, tmp_table):
    with sample_dag:
        output_table = tmp_table
        loaded_table = aql.load_file(
            str(CWD) + "/data/homes.csv", output_table=output_table
        )
        tranformed_table = do_a_thing(loaded_table)
        run_dataframe_funcs(tranformed_table)
        run_append(output_table)
        run_merge(output_table)
        run_validation(tranformed_table)
    test_utils.run_dag(sample_dag)
