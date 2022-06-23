import copy
import os
import time

import pandas as pd
from airflow.exceptions import BackfillUnfinished
from airflow.executors.debug_executor import DebugExecutor
from airflow.models.dag import DAG
from airflow.models.taskinstance import TaskInstance
from airflow.utils import timezone
from airflow.utils.state import State
from pandas.testing import assert_frame_equal

from astro.sql.table import Metadata

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

SQL_SERVER_HOOK_PARAMETERS = {
    "snowflake": {
        "snowflake_conn_id": "snowflake_conn",
        "metadata": Metadata(
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
        ),
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


def get_default_parameters(database_name):
    # While hooks expect specific attributes for connection (e.g. `snowflake_conn_id`)
    # the load_file operator expects a generic attribute name (`conn_id`)
    sql_server_params = copy.deepcopy(SQL_SERVER_HOOK_PARAMETERS[database_name])
    conn_id_value = sql_server_params.pop(SQL_SERVER_CONNECTION_KEY[database_name])
    sql_server_params["conn_id"] = conn_id_value
    return sql_server_params


def get_table_name(prefix):
    """get unique table name"""
    return prefix + "_" + str(int(time.time()))


def run_dag(dag: DAG, account_for_cleanup_failure=False):
    """

    :param dag: DAG
    :param account_for_cleanup_failure: Since our cleanup task fails on purpose when running in 'single thread mode'
    we account for this by running the backfill one more time if the cleanup task is the ONLY failed task. Otherwise
    we just passthrough the exception.
    :return:
    """
    dag.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, dag_run_state=State.NONE)
    try:
        dag.run(
            executor=DebugExecutor(),
            start_date=DEFAULT_DATE,
            end_date=DEFAULT_DATE,
            run_at_least_once=True,
        )
    except BackfillUnfinished as b:
        if not account_for_cleanup_failure:
            raise b
        failed_tasks = b.ti_status.failed

        if len(failed_tasks) != 1 or list(failed_tasks)[0].task_id != "_cleanup":
            raise b
        ti_key = list(failed_tasks)[0]

        # Cleanup now that everything is done
        ti = TaskInstance(task=dag.get_task("_cleanup"), run_id=ti_key.run_id)
        ti = ti.task.execute({"ti": ti, "dag_run": ti.get_dagrun()})


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
