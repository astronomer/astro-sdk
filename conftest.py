import os
import pathlib
import random
import string
import uuid

import pytest
import yaml
from airflow import settings
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session

from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable
from astro.utils.cloud_storage_creds import parse_s3_env_var
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook
from tests.operators import utils as test_utils

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")
UNIQUE_HASH_SIZE = 16
CWD = pathlib.Path(__file__).parent


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
        create_default_connections(session)
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
def output_table(request):
    table_type = request.param
    if table_type == "None":
        return TempTable()
    elif table_type == "partial":
        return Table("my_table")
    elif table_type == "full":
        return Table("my_table", database="pagila", conn_id="postgres_conn")


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


@pytest.fixture
def remote_file(request):
    param = request.param
    provider = param["name"]
    no_of_files = param["count"] if "count" in param else 1

    if provider == "google":
        conn_id = "test_google"
        conn_type = "google_cloud_platform"
        extra = {
            "extra__google_cloud_platform__key_path": os.getenv(
                "GOOGLE_APPLICATION_CREDENTIALS"
            )
        }
    elif provider == "amazon":
        key, secret = parse_s3_env_var()
        conn_id = "test_amazon"
        conn_type = "S3"
        extra = {"aws_access_key_id": key, "aws_secret_access_key": secret}
    else:
        raise ValueError(f"File location {request.param} not supported")

    new_connection = Connection(conn_id=conn_id, conn_type=conn_type, extra=extra)
    session = settings.Session()
    if not (
        session.query(Connection)
        .filter(Connection.conn_id == new_connection.conn_id)
        .first()
    ):
        session.add(new_connection)
        session.commit()

    filename = pathlib.Path("tests/data/sample.csv")
    object_paths = []
    unique_value = uuid.uuid4()
    for count in range(no_of_files):
        object_prefix = f"test/{unique_value}__{count}.csv"
        if provider == "google":
            bucket_name = os.getenv("GOOGLE_BUCKET", "dag-authoring")
            hook = GCSHook(gcp_conn_id=conn_id)
            hook.upload(bucket_name, object_prefix, filename)
            object_path = f"gs://{bucket_name}/{object_prefix}"
        else:
            bucket_name = os.getenv("AWS_BUCKET", "tmp9")
            hook = S3Hook(aws_conn_id=conn_id)
            hook.load_file(filename, object_prefix, bucket_name)
            object_path = f"s3://{bucket_name}/{object_prefix}"

        object_paths.append(object_path)

    yield conn_id, object_paths

    for count in range(no_of_files):
        object_prefix = f"test/{unique_value}__{count}.csv"
        if provider == "google":
            if hook.exists(bucket_name, object_prefix):
                hook.delete(bucket_name, object_prefix)
        else:
            if hook.check_for_prefix(
                object_prefix, delimiter="/", bucket_name=bucket_name
            ):
                hook.delete_objects(bucket_name, object_prefix)

    session.delete(new_connection)
    session.flush()
