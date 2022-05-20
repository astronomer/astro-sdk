import os
import pathlib
import uuid

import pandas as pd
import pytest
import yaml
from airflow.hooks.base import BaseHook
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session, provide_session

from astro.constants import Database, FileLocation, FileType
from astro.databases import create_database
from astro.files import File
from astro.sql.table import Table, create_unique_table_name
from astro.utils.dependencies import gcs, s3
from astro.utils.load import load_dataframe_into_sql_table

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

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
    # FIXME: Delete this fixture by the end of the refactoring! Use database_table_fixture instead
    tables = []
    tables_params = [{"is_temp": True}]

    if getattr(request, "param", None):
        if isinstance(request.param, list):
            tables_params = request.param
        else:
            tables_params = [request.param]

    sql_name, hook = sql_server

    database_name_to_conn_id = {
        Database.SQLITE.value: "sqlite_default",
        Database.BIGQUERY.value: "google_cloud_default",
        Database.POSTGRES.value: "postgres_conn",
        Database.SNOWFLAKE.value: "snowflake_conn",
    }
    conn_id = database_name_to_conn_id[sql_name]
    database = create_database(conn_id)

    for table_param in tables_params:
        load_table = table_param.get("load_table", False)
        t = Table(conn_id=conn_id, metadata=database.default_metadata)
        if load_table:
            database.load_file_to_table(
                source_file=File(table_param.get("path")), target_table=t
            )
        tables.append(t)

    yield tables if len(tables) > 1 else tables[0]

    for table in tables:
        hook = database.hook
        database.drop_table(table)
        if database == Database.SQLITE:
            hook.run("DROP INDEX IF EXISTS unique_index")
        elif database in (Database.POSTGRES, Database.POSTGRESQL):
            # There are some tests (e.g. test_agnostic_merge.py) which create stuff which are not being deleted
            # Example: tables which are not fixtures and constraints.
            # This is an aggressive approach towards tearing down:
            hook.run(f"DROP SCHEMA IF EXISTS {table.metadata.schema} CASCADE")


@pytest.fixture
def output_table(request):
    table_type = request.param
    if table_type == "None":
        return Table()
    elif table_type == "partial":
        return Table(name="my_table")
    elif table_type == "full":
        return Table(name="my_table", conn_id="postgres_conn_pagila")


@pytest.fixture
def sql_server(request):
    # FIXME: delete this fixture by the end of the refactoring! Use database_table_fixture instead
    sql_name = request.param
    database_name_to_conn_id = {
        Database.SQLITE.value: "sqlite_default",
        Database.BIGQUERY.value: "google_cloud_default",
        Database.POSTGRES.value: "postgres_conn",
        Database.SNOWFLAKE.value: "snowflake_conn",
    }
    conn_id = database_name_to_conn_id[sql_name]
    database = create_database(conn_id)
    t = Table(conn_id=conn_id, metadata=database.default_metadata)
    database.drop_table(t)
    yield sql_name, database.hook
    database.drop_table(t)


@pytest.fixture
def schema_fixture(request, sql_server):
    """
    Given request.param in the format:
        "someschema"  # name of the schema to be created

    If the schema exists, it is deleted during the tests setup and tear down.
    """
    schema_name = request.param
    _, hook = sql_server

    hook.run(f"DROP SCHEMA IF EXISTS {schema_name}")
    yield

    hook.run(f"DROP SCHEMA IF EXISTS {schema_name}")


@pytest.fixture
def database_table_fixture(request):
    """
    Given request.param in the format:
        {
            "database": Database.SQLITE,  # mandatory, may be any supported database
            "table": astro.sql.tables.Table(),  # optional, will create a table unless it is passed
            "filepath": ""  # optional, filepath to be used to load data to the table.
        }
    This fixture yields the following during setup:
        (database, table)
    Example:
        (astro.databases.sqlite.SqliteDatabase(), Table())

    If the table exists, it is deleted during the tests setup and tear down.
    The table will only be created during setup if request.param contains the `filepath` parameter.
    """
    params = request.param
    file = params.get("file", None)

    database_name = params["database"]
    database_name_to_conn_id = {
        Database.SQLITE: "sqlite_default",
        Database.BIGQUERY: "google_cloud_default",
        Database.POSTGRES: "postgres_conn",
        Database.SNOWFLAKE: "snowflake_conn",
    }
    conn_id = database_name_to_conn_id[database_name]
    database = create_database(conn_id)

    table = params.get(
        "table", Table(conn_id=conn_id, metadata=database.default_metadata)
    )
    database.create_schema_if_needed(table.metadata.schema)
    database.drop_table(table)
    if file:
        database.load_file_to_table(file, table)
    yield database, table

    database.drop_table(table)


@pytest.fixture
def remote_files_fixture(request):
    """
    Return a list of remote object filenames.
    By default, this fixture also creates objects using sample.<filetype>, unless
    the user uses file_create=false.

    Given request.param in the format:
        {
            "provider": "google",  # mandatory, may be "google" or "amazon"
            "file_count": 2,  # optional, in case the user wants to create multiple files
            "filetype": FileType.CSV  # optional, defaults to .csv if not given,
            "file_create": False
        }
    Yield the following during setup:
        [object1_uri, object2_uri]
    Example:
        [
            "gs://some-bucket/test/8df8aea0-9b2e-4671-b84e-2d48f42a182f0.csv",
            "gs://some-bucket/test/8df8aea0-9b2e-4671-b84e-2d48f42a182f1.csv"
        ]

    If the objects exist, they are deleted during the tests setup and tear down.
    """
    params = request.param
    provider = params["provider"]
    file_count = params.get("file_count", 1)
    file_extension = params.get("filetype", FileType.CSV).value
    file_create = params.get("file_create", True)

    source_path = pathlib.Path(f"{CWD}/tests/data/sample.{file_extension}")

    object_path_list = []
    object_prefix_list = []
    unique_value = uuid.uuid4()
    for item_count in range(file_count):
        object_prefix = f"test/{unique_value}{item_count}.{file_extension}"
        if provider == "google":
            bucket_name = os.getenv("GOOGLE_BUCKET", "dag-authoring")
            object_path = f"gs://{bucket_name}/{object_prefix}"
            hook = gcs.GCSHook()
            if file_create:
                hook.upload(bucket_name, object_prefix, source_path)
            else:
                # if an object doesn't exist, GCSHook.delete fails:
                hook.exists(  # skipcq: PYL-W0106
                    bucket_name, object_prefix
                ) and hook.delete(bucket_name, object_prefix)
        else:
            bucket_name = os.getenv("AWS_BUCKET", "tmp9")
            object_path = f"s3://{bucket_name}/{object_prefix}"
            hook = s3.S3Hook()
            if file_create:
                hook.load_file(source_path, object_prefix, bucket_name)
            else:
                hook.delete_objects(bucket_name, object_prefix)
        object_path_list.append(object_path)
        object_prefix_list.append(object_prefix)

    yield object_path_list

    if provider == "google":
        for object_prefix in object_prefix_list:
            # if an object doesn't exist, GCSHook.delete fails:
            hook.exists(  # skipcq: PYL-W0106
                bucket_name, object_prefix
            ) and hook.delete(bucket_name, object_prefix)
    else:
        for object_prefix in object_prefix_list:
            hook.delete_objects(bucket_name, object_prefix)


def method_map_fixture(method, base_path, classes, get):
    """Generic method to generate paths to method/property with a package."""
    filetype_to_class = {get(cls): f"{base_path[0]}.{cls}.{method}" for cls in classes}
    return filetype_to_class


@pytest.fixture
def type_method_map_fixture(request):
    """Get paths for type's package for methods"""
    method = request.param["method"]
    classes = ["JSONFileType", "CSVFileType", "NDJSONFileType", "ParquetFileType"]
    base_path = ("astro.files.type",)
    suffix = "FileType"

    yield method_map_fixture(
        method=method,
        classes=classes,
        base_path=base_path,
        get=lambda x: FileType(x.rstrip(suffix).lower()),
    )


@pytest.fixture
def locations_method_map_fixture(request):
    """Get paths for location's package for methods"""
    method = request.param["method"]
    classes = [
        "local.LocalLocation",
        "http.HTTPLocation",
        "google.gcs.GCSLocation",
        "amazon.s3.S3Location",
    ]
    base_path = ("astro.files.locations",)
    suffix = "Location"

    synonyms = {"gcs": "gs"}

    def get_location_type(cls):
        """getter for location type"""
        val = cls.split(".")[-1].rstrip(suffix).lower()
        if synonyms.get(val):
            val = synonyms[val]
        return FileLocation(val)

    result = method_map_fixture(
        method=method, classes=classes, base_path=base_path, get=get_location_type
    )
    result[FileLocation("https")] = result[FileLocation("http")]
    yield result
