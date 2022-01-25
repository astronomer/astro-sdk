import os
import time

from airflow.executors.debug_executor import DebugExecutor
from airflow.models.taskinstance import State
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.state import State
from airflow.utils.types import DagRunType

DEFAULT_SCHEMA = "tmp_astro"
DEFAULT_DATE = timezone.datetime(2016, 1, 1)

SQL_SERVER_HOOK_PARAMETERS = {
    "snowflake": {
        "snowflake_conn_id": "snowflake_conn",
        "schema": os.getenv("SNOWFLAKE_SCHEMA"),
        "database": os.getenv("SNOWFLAKE_DATABASE"),
        "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
    },
    "postgres": {"postgres_conn_id": "postgres_conn"},
}
SQL_SERVER_CONNECTION_KEY = {
    "snowflake": "snowflake_conn_id",
    "postgres": "postgres_conn_id",
}

SQL_SERVER_HOOK_CLASS = {
    "snowflake": SnowflakeHook,
    "postgres": PostgresHook,
}


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
    schema: str = os.environ["SNOWFLAKE_SCHEMA"],
    database: str = os.environ["SNOWFLAKE_DATABASE"],
    warehouse: str = os.environ["SNOWFLAKE_WAREHOUSE"],
):
    hook = SnowflakeHook(
        snowflake_conn_id=conn_id,
        schema=schema,
        database=database,
        warehouse=warehouse,
    )
    snowflake_conn = hook.get_conn()
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
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
