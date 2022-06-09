import os
import pathlib
import uuid

import pytest
import yaml
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session, provide_session

from astro.constants import Database, FileLocation, FileType
from astro.databases import create_database
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table, create_unique_table_name
from astro.utils.database import get_database_name
from astro.utils.dependencies import BigQueryHook, gcs, s3
from tests.sql.operators import utils as test_utils

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

OUTPUT_TABLE_NAME = test_utils.get_table_name("integration_test_table")
UNIQUE_HASH_SIZE = 16
CWD = pathlib.Path(__file__).parent

DATABASE_NAME_TO_CONN_ID = {
    Database.SQLITE: "sqlite_default",
    Database.BIGQUERY: "google_cloud_default",
    Database.POSTGRES: "postgres_conn",
    Database.SNOWFLAKE: "snowflake_conn",
}


@provide_session
def get_session(session=None):  # skipcq:  PYL-W0621
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


@pytest.fixture(scope="session", autouse=True)
def create_database_connections():
    with open(os.path.dirname(__file__) + "/test-connections.yaml") as fp:
        yaml_with_env = os.path.expandvars(fp.read())
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
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()


@pytest.fixture
def sample_astro_dag():
    from astro.dag import DAG as AstroDAG

    dag_id = create_unique_table_name(UNIQUE_HASH_SIZE)
    yield AstroDAG(
        dag_id, default_args={"owner": "airflow", "start_date": DEFAULT_DATE}
    )
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()


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
    database = get_database_name(hook)

    for table_param in tables_params:
        load_table = table_param.get("load_table", False)
        override_table_options = table_param.get("param", {})

        if database == Database.SNOWFLAKE:
            default_table_options = {
                "conn_id": hook.snowflake_conn_id,
                "metadata": Metadata(
                    database=hook.database,
                    schema=os.getenv("SNOWFLAKE_SCHEMA") or SCHEMA,
                ),
            }
        elif database == Database.POSTGRES:
            default_table_options = {
                "conn_id": hook.postgres_conn_id,
                "metadata": Metadata(schema=SCHEMA),
            }
        elif database == Database.SQLITE:
            default_table_options = {"conn_id": hook.sqlite_conn_id}
        elif database == Database.BIGQUERY:
            default_table_options = {
                "conn_id": hook.gcp_conn_id,
                "metadata": Metadata(schema=SCHEMA),
            }
        else:
            raise ValueError("Unsupported Database")

        default_table_options.update(override_table_options)
        tables.append(Table(**default_table_options))
        if load_table:
            table = tables[-1]
            db = create_database(table.conn_id)
            db.load_file_to_table(
                source_file=File(path=table_param.get("path")), target_table=table
            )

    yield tables if len(tables) > 1 else tables[0]

    for table in tables:
        db = create_database(table.conn_id)
        hook.run(f"DROP TABLE IF EXISTS {db.get_table_qualified_name(table)}")

        if database == Database.SQLITE:
            hook.run("DROP INDEX IF EXISTS unique_index")
        elif database in (Database.POSTGRES, Database.POSTGRESQL):
            # There are some tests (e.g. test_agnostic_merge.py) which create stuff which are not being deleted
            # Example: tables which are not fixtures and constraints.
            # This is an aggressive approach towards tearing down:
            schema = getattr(table.metadata, "schema", None)
            if schema:
                hook.run(f"DROP SCHEMA IF EXISTS {table.metadata.schema} CASCADE;")


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
    hook_parameters = test_utils.SQL_SERVER_HOOK_PARAMETERS.get(sql_name)
    hook_class = test_utils.SQL_SERVER_HOOK_CLASS.get(sql_name)
    if hook_parameters is None or hook_class is None:
        raise ValueError(f"Unsupported SQL server {sql_name}")
    hook = hook_class(**hook_parameters)
    schema = hook_parameters.get("metadata", Metadata()).schema or SCHEMA
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")
    yield (sql_name, hook)
    if not isinstance(hook, BigQueryHook):
        hook.run(f"DROP TABLE IF EXISTS {schema}.{OUTPUT_TABLE_NAME}")


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
            "file": ""  # optional, File() instance to be used to load data to the table.
        }
    This fixture yields the following during setup:
        (database, table)
    Example:
        (astro.databases.sqlite.SqliteDatabase(), Table())

    If the table exists, it is deleted during the tests setup and tear down.
    The table will only be created during setup if request.param contains the `file` parameter.
    """
    params = request.param
    file = params.get("file", None)

    database_name = params["database"]
    conn_id = DATABASE_NAME_TO_CONN_ID[database_name]
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
def tables_fixture(request, database_table_fixture):
    """
    Given request.param in the format:
    {
        "items": [
            {
                "table": Table(),  # optional
                "file": File()  # optional
            }
        ]
    }
    If the table key is missing, the fixture creates a table using the database.conn_id.

    For each table in the list, if the table exists, it is deleted during the tests setup and tear down.
    The table will only be created during setup if the item contains the "file" to be loaded to the table.
    """
    database, _ = database_table_fixture
    items = request.param.get("items", [])
    tables_list = []
    for item in items:
        table = item.get("table", Table(conn_id=database.conn_id))
        database.populate_table_metadata(table)
        file = item.get("file", None)
        database.drop_table(table)
        if file:
            database.load_file_to_table(file, table)
        tables_list.append(table)

    yield tables_list

    for table in tables_list:
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
    base_path = ("astro.files.types",)
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
