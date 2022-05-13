import copy
import os
import time
from typing import Optional

import pandas as pd
from airflow.executors.debug_executor import DebugExecutor
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils import timezone
from airflow.utils.state import State
from pandas.testing import assert_frame_equal

from astro.databases import create_database
from astro.sql.tables import Table
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook, bigquery

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

SQL_SERVER_HOOK_PARAMETERS = {
    "snowflake": {
        "snowflake_conn_id": "snowflake_conn",
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    },
    "postgres": {"postgres_conn_id": "postgres_conn"},
    "bigquery": {"gcp_conn_id": "google_cloud_default", "use_legacy_sql": False},
    "sqlite": {"sqlite_conn_id": "sqlite_conn"},
}
SQL_SERVER_CONNECTION_KEY = {
    "snowflake": "snowflake_conn_id",
    "postgres": "postgres_conn_id",
    "bigquery": "gcp_conn_id",
    "sqlite": "sqlite_conn_id",
}

SQL_SERVER_HOOK_CLASS = {
    "snowflake": SnowflakeHook,
    "postgres": PostgresHook,
    "bigquery": BigQueryHook,
    "sqlite": SqliteHook,
}


def get_default_parameters(database_name):
    # While hooks expect specific attributes for connection (e.g. `snowflake_conn_id`)
    # the load_file operator expects a generic attribute name (`conn_id`)
    sql_server_params = copy.deepcopy(SQL_SERVER_HOOK_PARAMETERS[database_name])
    conn_id_value = sql_server_params.pop(SQL_SERVER_CONNECTION_KEY[database_name])
    sql_server_params["conn_id"] = conn_id_value
    return sql_server_params


def create_and_run_task(dag, decorator_func, op_args, op_kwargs):
    with dag:
        function = decorator_func(*op_args, **op_kwargs)
    run_dag(dag)
    return function


def get_table_name(prefix):
    """get unique table name"""
    return prefix + "_" + str(int(time.time()))


def drop_table_snowflake(
    table_name: str,
    conn_id: str = "snowflake_conn",
    schema: Optional[str] = os.getenv("SNOWFLAKE_SCHEMA"),
    database: Optional[str] = os.getenv("SNOWFLAKE_DATABASE"),
    warehouse: Optional[str] = os.getenv("SNOWFLAKE_WAREHOUSE"),
):
    hook = SnowflakeHook(
        snowflake_conn_id=conn_id,
        schema=schema,
        database=database,
        warehouse=warehouse,
    )
    snowflake_conn = hook.get_conn()
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {schema}.{table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


def drop_table_postgres(
    table_name: str, conn_id: str = "postgres_conn", schema: str = "postgres"
):
    hook = PostgresHook(postgres_conn_id=conn_id, schema=schema)
    postgres_conn = hook.get_conn()
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


def run_dag(dag):
    dag.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, dag_run_state=State.NONE)

    dag.run(
        executor=DebugExecutor(),
        start_date=DEFAULT_DATE,
        end_date=DEFAULT_DATE,
        run_at_least_once=True,
    )


def get_dataframe_from_table(sql_name: str, test_table: Table, hook):
    db = create_database(test_table.conn_id)
    if sql_name == "bigquery":
        client = bigquery.Client()
        query_job = client.query(
            f"SELECT * FROM astronomer-dag-authoring.{db.get_table_qualified_name(test_table)}"
        )
        df = query_job.to_dataframe()
    else:
        df = pd.read_sql(
            f"SELECT * FROM {db.get_table_qualified_name(test_table)}",
            con=hook.get_conn(),
        )
    return df


def load_to_dataframe(filepath, file_type):
    read = {
        "parquet": pd.read_parquet,
        "csv": pd.read_csv,
        "json": pd.read_json,
        "ndjson": pd.read_json,
    }
    read_params = {"ndjson": {"lines": True}}
    mode = {"parquet": "rb"}
    with open(filepath, mode.get(file_type, "r")) as fp:
        return read[file_type](fp, **read_params.get(file_type, {}))


def assert_dataframes_are_equal(df: pd.DataFrame, expected: pd.DataFrame) -> None:
    """
    Auxiliary function to compare similarity of dataframes to avoid repeating this logic in many tests.
    """
    df = df.rename(columns=str.lower)
    df = df.astype({"id": "int64"})
    expected = expected.astype({"id": "int64"})
    assert_frame_equal(df, expected)
