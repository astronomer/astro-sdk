import logging
import os
import pathlib
import random
import string
import uuid
from copy import deepcopy

import pytest
import yaml
from airflow.models import Connection, DagRun, TaskInstance as TI
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session, provide_session

from astro.constants import Database, FileType
from astro.databases import create_database
from astro.databases.databricks.load_options import DeltaLoadOptions
from astro.table import MAX_TABLE_NAME_LENGTH, Table, TempTable

CWD = pathlib.Path(__file__).parent
UNIQUE_HASH_SIZE = 16
DATABASE_NAME_TO_CONN_ID = {
    Database.SQLITE: "sqlite_default",
    Database.BIGQUERY: "google_cloud_default",
    Database.POSTGRES: "postgres_conn",
    Database.SNOWFLAKE: "snowflake_conn",
    Database.REDSHIFT: "redshift_conn",
    Database.DELTA: "databricks_conn",
    Database.MSSQL: "mssql_conn",
    Database.DUCKDB: "duckdb_conn",
    Database.MYSQL: "mysql_conn",
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
    with open(os.path.dirname(__file__) + "/../test-connections.yaml") as fp:
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
            last_conn = session.query(Connection).filter(Connection.conn_id == conn.conn_id).first()
            if last_conn is not None:
                session.delete(last_conn)
                session.flush()
                logging.info(
                    f"Overriding existing conn_id {conn.conn_id} "
                    f"with connection specified in test_connections.yaml"
                )
            session.add(conn)


@pytest.fixture
def database_temp_table_fixture(request):
    """
    Given request.param in the format:
        {
            "database": Database.SQLITE,  # mandatory, may be any supported database
        }
    This fixture yields the following during setup:
        (database, temp_table)
    Example:
        (astro.databases.sqlite.SqliteDatabase(), TempTable())
    """
    params = request.param

    database_name = params["database"]
    conn_id = DATABASE_NAME_TO_CONN_ID[database_name]
    database = create_database(conn_id)
    temp_table = TempTable(conn_id=database.conn_id)

    database.populate_table_metadata(temp_table)
    database.create_schema_if_applicable(temp_table.metadata.schema)
    yield database, temp_table
    database.drop_table(temp_table)


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
    # We deepcopy the request param dictionary as we modify the table item directly.
    params = deepcopy(request.param)

    items = params.get("items", [])
    tables = []

    for item in items:
        table = item.get("table", Table(conn_id=database.conn_id))
        if not isinstance(table, TempTable):
            # We create a unique table name to make the name unique across runs
            table.name = create_unique_table_name(UNIQUE_HASH_SIZE)
        file = item.get("file")

        database.populate_table_metadata(table)
        database.create_schema_if_applicable(table.metadata.schema)
        if file:
            database.load_file_to_table(
                file,
                table,
                load_options=[DeltaLoadOptions.get_default_delta_options()],
            )
        tables.append(table)

    yield tables if len(tables) > 1 else tables[0]

    for table in tables:
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

    source_path = pathlib.Path(f"{CWD}/data/sample.{file_extension}")

    object_path_list = []
    object_prefix_list = []
    unique_value = uuid.uuid4()
    get_bucket_name(provider)
    get_hook(provider)
    for item_count in range(file_count):
        object_prefix = f"test/{unique_value}{item_count}.{file_extension}"
        object_path = _upload_or_delete_remote_file(file_create, object_prefix, provider, source_path)
        object_path_list.append(object_path)
        object_prefix_list.append(object_prefix)

    yield object_path_list

    delete_all_blobs(provider=provider, object_prefix_list=object_prefix_list)


def create_blob(provider, source_path, bucket_name, object_prefix):
    hook = get_hook(provider)
    if provider == "google":
        hook.upload(bucket_name, object_prefix, source_path)
    elif provider == "amazon":
        hook.load_file(source_path, object_prefix, bucket_name)
    elif provider == "azure":
        hook.load_file(source_path, bucket_name, object_prefix)


def delete_all_blobs(provider: str, object_prefix_list: list):
    hook = get_hook(provider)
    bucket_name = get_bucket_name(provider)
    delete_blob_provider = {
        "google": delete_google_blob,
        "amazon": delete_amazon_blob,
        "azure": delete_azure_blob,
        "local": delete_local_blob,
    }
    delete_blob_fun = delete_blob_provider[provider]
    for object_prefix in object_prefix_list:
        delete_blob_fun(hook, bucket_name, object_prefix)


def delete_google_blob(hook, bucket_name, object_prefix):
    hook.exists(bucket_name, object_prefix) and hook.delete(bucket_name, object_prefix)  # skipcq: PYL-W0106


def delete_amazon_blob(hook, bucket_name, object_prefix):
    hook.delete_objects(bucket_name, object_prefix)


def delete_azure_blob(hook, bucket_name, object_prefix):
    hook.check_for_blob(bucket_name, object_prefix) and hook.delete_file(  # skipcq: PYL-W0106
        bucket_name, object_prefix
    )


def delete_local_blob(hook, bucket_name, object_prefix):
    pass


def get_bucket_name(provider):
    bucket_provider = {
        "google": os.getenv("GOOGLE_BUCKET", "dag-authoring"),
        "amazon": os.getenv("AWS_BUCKET", "tmp9"),
        "azure": os.getenv("AZURE_BUCKET", "astro-sdk"),
        "local": None,
    }
    return bucket_provider[provider]


def get_object_path(provider: str, object_prefix: str, source_path) -> str:
    bucket_name = get_bucket_name(provider)
    path_provider = {
        "google": f"gs://{bucket_name}/{object_prefix}",
        "amazon": f"s3://{bucket_name}/{object_prefix}",
        "azure": f"wasb://{bucket_name}/{object_prefix}",
        "local": str(source_path),
    }
    return path_provider[provider]


def get_hook(provider):
    from airflow.providers.amazon.aws.hooks.s3 import S3Hook
    from airflow.providers.google.cloud.hooks.gcs import GCSHook
    from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

    hook_provider = {
        "google": GCSHook(),
        "amazon": S3Hook(),
        "azure": WasbHook(),
        "local": None,
    }
    return hook_provider[provider]


def _upload_or_delete_remote_file(file_create, object_prefix, provider, source_path):
    """
    Upload a local file to remote bucket if file_create is True.
    And deletes a file if it already exists and file_create is False.
    """
    bucket_name = get_bucket_name(provider=provider)
    object_path = get_object_path(provider=provider, object_prefix=object_prefix, source_path=source_path)
    if file_create:
        create_blob(
            provider=provider, bucket_name=bucket_name, object_prefix=object_prefix, source_path=source_path
        )
    else:
        delete_all_blobs(provider, [object_prefix])

    return object_path


def method_map_fixture(method, base_path, classes, get):
    """Generic method to generate paths to method/property with a package."""
    filetype_to_class = {get(cls): f"{base_path[0]}.{cls}.{method}" for cls in classes}
    return filetype_to_class


@pytest.fixture
def type_method_map_fixture(request):
    """Get paths for type's package for methods"""
    method = request.param["method"]
    classes = ["JSONFileType", "CSVFileType", "NDJSONFileType", "ParquetFileType", "XLSXFileType", "XLSFileType"]
    base_path = ("astro.files.types",)

    yield method_map_fixture(
        method=method,
        classes=classes,
        base_path=base_path,
        get=lambda x: FileType(x[0:-8].lower()),  # remove FileType suffix
    )
