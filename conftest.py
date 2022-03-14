import os
import random
import string

import pytest
import yaml
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session

from astro.settings import SCHEMA
from astro.sql.table import TempTable
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook
from tests.operators import utils as test_utils

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")
UNIQUE_HASH_SIZE = 16


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


def _create_unique_dag_id():
    # To use 32 chars was too long for the DAG & SQL name table, for this reason we are not using:
    # unique_id = str(uuid.uuid4()).replace("-", "")
    unique_id = "".join(
        random.choice(string.ascii_lowercase + string.digits)
        for _ in range(UNIQUE_HASH_SIZE)
    )
    return f"test_dag_{unique_id}"


@pytest.fixture
def sample_dag():
    dag_id = _create_unique_dag_id()
    yield DAG(dag_id, default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()


@pytest.fixture
def tmp_table(sql_server):
    sql_name, hook = sql_server

    if isinstance(hook, SnowflakeHook):
        temporary_table = TempTable(
            conn_id=hook.snowflake_conn_id,
            database=hook.database,
            warehouse=hook.warehouse,
        )
    elif isinstance(hook, PostgresHook):
        temporary_table = TempTable(conn_id=hook.postgres_conn_id, database=hook.schema)
    elif isinstance(hook, SqliteHook):
        temporary_table = TempTable(conn_id=hook.sqlite_conn_id, database="sqlite")
    elif isinstance(hook, BigQueryHook):
        temporary_table = TempTable(conn_id=hook.gcp_conn_id)
    yield temporary_table

    if isinstance(hook, SqliteHook):
        hook.run(f"DROP TABLE IF EXISTS {temporary_table.table_name}")
        hook.run(f"DROP INDEX IF EXISTS unique_index")
    elif isinstance(hook, SnowflakeHook):
        hook.run(f"DROP TABLE IF EXISTS {temporary_table.table_name}")
    elif not isinstance(hook, BigQueryHook):
        # There are some tests (e.g. test_agnostic_merge.py) which create stuff which are not being deleted
        # Example: tables which are not fixtures and constraints. This is an agressive approach towards tearing down:
        hook.run(f"DROP SCHEMA IF EXISTS {temporary_table.schema} CASCADE;")


@pytest.fixture
def sql_server(request):
    sql_name = request.param
    hook_parameters = test_utils.SQL_SERVER_HOOK_PARAMETERS.get(sql_name)
    hook_class = test_utils.SQL_SERVER_HOOK_CLASS.get(sql_name)
    if hook_parameters is None or hook_class is None:
        raise ValueError(f"Unsupported SQL server {sql_name}")
    hook = hook_class(**hook_parameters)
    schema = hook_parameters.get("schema", SCHEMA)
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")
    yield (sql_name, hook)
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")
