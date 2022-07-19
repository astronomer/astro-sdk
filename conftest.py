import os
import pathlib
import random
import string
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
from astro.sql.table import MAX_TABLE_NAME_LENGTH, Table
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
    yield DAG(dag_id, start_date=DEFAULT_DATE)
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()


def create_unique_table_name(length: int = MAX_TABLE_NAME_LENGTH) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.

    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id


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
def schemas_fixture(request, database_table_fixture):
    """
    Given request.param in the format:
        "someschema"  # name of the schema to be created

    If the schema exists, it is deleted during the tests setup and tear down.
    """
    schema_name = request.param
    database, _ = database_table_fixture

    database.run_sql(f"DROP SCHEMA IF EXISTS {schema_name}")

    yield

    database.run_sql(f"DROP SCHEMA IF EXISTS {schema_name}")


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
    table.conn_id = table.conn_id or conn_id
    if table.metadata.is_empty():
        table.metadata = database.default_metadata
    database.create_schema_if_needed(table.metadata.schema)

    database.drop_table(table)
    if file:
        database.load_file_to_table(file, table, {})
    yield database, table

    database.drop_table(table)


@pytest.fixture
def multiple_tables_fixture(request, database_table_fixture):
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
        table = database.populate_table_metadata(table)
        file = item.get("file", None)
        database.drop_table(table)
        if file:
            database.load_file_to_table(file, table, {})
        tables_list.append(table)

    yield tables_list if len(tables_list) > 1 else tables_list[0]

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
        bucket_name, hook, object_path = _upload_or_delete_remote_file(
            file_create, object_prefix, provider, source_path
        )
        object_path_list.append(object_path)
        object_prefix_list.append(object_prefix)

    yield object_path_list

    if provider == "google":
        for object_prefix in object_prefix_list:
            # if an object doesn't exist, GCSHook.delete fails:
            hook.exists(  # skipcq: PYL-W0106
                bucket_name, object_prefix
            ) and hook.delete(bucket_name, object_prefix)
    elif provider == "amazon":
        for object_prefix in object_prefix_list:
            hook.delete_objects(bucket_name, object_prefix)


def _upload_or_delete_remote_file(file_create, object_prefix, provider, source_path):
    """
    Upload a local file to remote bucket if file_create is True.
    And deletes a file if it already exists and file_create is False.
    """
    if provider == "google":
        from airflow.providers.google.cloud.hooks.gcs import GCSHook

        bucket_name = os.getenv("GOOGLE_BUCKET", "dag-authoring")
        object_path = f"gs://{bucket_name}/{object_prefix}"
        hook = GCSHook()
        if file_create:
            hook.upload(bucket_name, object_prefix, source_path)
        else:
            # if an object doesn't exist, GCSHook.delete fails:
            hook.exists(  # skipcq: PYL-W0106
                bucket_name, object_prefix
            ) and hook.delete(bucket_name, object_prefix)
    elif provider == "amazon":
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        bucket_name = os.getenv("AWS_BUCKET", "tmp9")
        object_path = f"s3://{bucket_name}/{object_prefix}"
        hook = S3Hook()
        if file_create:
            hook.load_file(source_path, object_prefix, bucket_name)
        else:
            hook.delete_objects(bucket_name, object_prefix)
    elif provider == "local":
        bucket_name = None
        hook = None
        object_path = str(source_path)
    return bucket_name, hook, object_path


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
