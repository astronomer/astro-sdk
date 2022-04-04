import os
import pathlib
import uuid

import pandas as pd
import pytest
import yaml
from airflow import settings
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session, provide_session

from astro.constants import Database, FileType
from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable, create_unique_table_name
from astro.utils.cloud_storage_creds import parse_s3_env_var
from astro.utils.database import get_database_name
from astro.utils.dependencies import BigQueryHook, gcs, s3
from astro.utils.load import load_dataframe_into_sql_table
from tests.operators import utils as test_utils

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")
UNIQUE_HASH_SIZE = 16
CWD = pathlib.Path(__file__).parent


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


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


@pytest.fixture
def sample_dag():
    dag_id = create_unique_table_name(UNIQUE_HASH_SIZE)
    yield DAG(dag_id, default_args={"owner": "airflow", "start_date": DEFAULT_DATE})
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()


def populate_table(path: str, table: Table, hook: BaseHook) -> None:
    """
    Populate a csv file into a sql table
    """
    df = pd.read_csv(path)
    load_dataframe_into_sql_table(pandas_dataframe=df, output_table=table, hook=hook)


@pytest.fixture
def test_table(request, sql_server):  # noqa: C901
    tables = []
    tables_params = [{"is_temp": True}]

    if getattr(request, "param", None):
        if isinstance(request.param, list):
            tables_params = request.param
        else:
            tables_params = [request.param]

    sql_name, hook = sql_server
    database = get_database_name(hook)

    for table_param in tables_params:
        is_tmp_table = table_param.get("is_temp", True)
        load_table = table_param.get("load_table", False)
        override_table_options = table_param.get("param", {})

        if is_tmp_table and load_table:
            raise ValueError(
                "Temp Table cannot be populated with data. Use 'is_temp=False' instead."
            )

        if database == Database.SNOWFLAKE:
            default_table_options = {
                "conn_id": hook.snowflake_conn_id,
                "database": hook.database,
                "warehouse": hook.warehouse,
                "schema": hook.schema,
            }
        elif database == Database.POSTGRES:
            default_table_options = {
                "conn_id": hook.postgres_conn_id,
                "database": hook.schema,
            }
        elif database == Database.SQLITE:
            default_table_options = {
                "conn_id": hook.sqlite_conn_id,
                "database": "sqlite",
            }
        elif database == Database.BIGQUERY:
            default_table_options = {"conn_id": hook.gcp_conn_id, "schema": SCHEMA}
        else:
            raise ValueError("Unsupported Database")

        default_table_options.update(override_table_options)
        tables.append(
            TempTable(**default_table_options)
            if is_tmp_table
            else Table(**default_table_options)
        )
        if load_table:
            populate_table(path=table_param.get("path"), table=tables[-1], hook=hook)

    yield tables if len(tables) > 1 else tables[0]

    for table in tables:
        if table.qualified_name():
            hook.run(f"DROP TABLE IF EXISTS {table.qualified_name()}")

        if database == Database.SQLITE:
            hook.run("DROP INDEX IF EXISTS unique_index")
        elif database in (Database.POSTGRES, Database.POSTGRESQL):
            # There are some tests (e.g. test_agnostic_merge.py) which create stuff which are not being deleted
            # Example: tables which are not fixtures and constraints.
            # This is an aggressive approach towards tearing down:
            hook.run(f"DROP SCHEMA IF EXISTS {table.schema} CASCADE;")


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
    no_of_files = param.get("count", 1)
    file_extension = param.get("filetype", FileType.CSV).value

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
        raise ValueError(f"File location {provider} not supported")

    new_connection = Connection(conn_id=conn_id, conn_type=conn_type, extra=extra)
    session = settings.Session()
    if not (
        session.query(Connection)
        .filter(Connection.conn_id == new_connection.conn_id)
        .first()
    ):
        session.add(new_connection)
        session.commit()

    filename = pathlib.Path(f"{CWD}/tests/data/sample.{file_extension}")
    object_paths = []
    unique_value = uuid.uuid4()
    for count in range(no_of_files):
        object_prefix = f"test/{unique_value}__{count}.{file_extension}"
        if provider == "google":
            bucket_name = os.getenv("GOOGLE_BUCKET", "dag-authoring")
            hook = gcs.GCSHook(gcp_conn_id=conn_id)
            hook.upload(bucket_name, object_prefix, filename)
            object_path = f"gs://{bucket_name}/{object_prefix}"
        else:
            bucket_name = os.getenv("AWS_BUCKET", "tmp9")
            hook = s3.S3Hook(aws_conn_id=conn_id)
            hook.load_file(filename, object_prefix, bucket_name)
            object_path = f"s3://{bucket_name}/{object_prefix}"

        object_paths.append(object_path)

    yield conn_id, object_paths

    for count in range(no_of_files):
        object_prefix = f"test/{unique_value}__{count}.{file_extension}"
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
