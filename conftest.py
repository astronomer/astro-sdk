import os

import pytest
import yaml
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from astro.sql.table import TempTable
from astro.utils.dependencies import PostgresHook, SnowflakeHook

from tests.operators import utils as test_utils

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")

@pytest.fixture(scope="session", autouse=True)
def create_database_connections():
    with open(os.path.dirname(__file__) + "/test-connections.yaml") as file:
        yaml_with_env = os.path.expandvars(file.read())
        yaml_dicts = yaml.safe_load(yaml_with_env)
        connections = []
        for i in yaml_dicts["connections"]:
            connections.append(Connection(**i))
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()
        session.query(Connection).delete()
        for conn in connections:
            session.add(conn)


@pytest.fixture
def sample_dag():
    yield DAG("test_dag", default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()


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