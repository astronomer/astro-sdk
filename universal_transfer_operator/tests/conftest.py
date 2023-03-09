import logging
import os
from copy import deepcopy
from urllib.parse import urlparse, urlunparse

import pytest
import smart_open
import yaml
from airflow.models import DAG, Connection, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session
from google.api_core.exceptions import NotFound
from utils.test_utils import create_unique_str

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers import DataProviders, create_dataprovider
from universal_transfer_operator.data_providers.filesystem.base import BaseFilesystemProviders
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
UNIQUE_HASH_SIZE = 16

DATASET_NAME_TO_CONN_ID = {
    "SqliteDataProvider": "sqlite_default",
    "SnowflakeDataProvider": "snowflake_conn",
    "BigqueryDataProvider": "google_cloud_default",
    "S3DataProvider": "aws_default",
    "GCSDataProvider": "google_cloud_default",
    "LocalDataProvider": None,
    "SFTPDataProvider": "sftp_conn",
}
DATASET_NAME_TO_PROVIDER_TYPE = {
    "SqliteDataProvider": "database",
    "SnowflakeDataProvider": "database",
    "BigqueryDataProvider": "database",
    "S3DataProvider": "file",
    "GCSDataProvider": "file",
    "LocalDataProvider": "file",
    "SFTPDataProvider": "file",
}


@pytest.fixture
def sample_dag():
    dag_id = create_unique_str(UNIQUE_HASH_SIZE)
    yield DAG(dag_id, start_date=DEFAULT_DATE)
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()


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
                    "Overriding existing conn_id %s with connection specified in test_connections.yaml",
                    conn.conn_id,
                )
            session.add(conn)


@pytest.fixture
def dataset_table_fixture(request):
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
    # We deepcopy the request param dictionary as we modify the table item directly.
    params = deepcopy(request.param)

    dataset_name = params["dataset"]
    user_table = params.get("table", None)
    transfer_mode = params.get("transfer_mode", TransferMode.NONNATIVE)
    conn_id = DATASET_NAME_TO_CONN_ID[dataset_name]
    if user_table and user_table.conn_id:
        conn_id = user_table.conn_id

    table = user_table or Table(conn_id=conn_id)
    if not table.conn_id:
        table.conn_id = conn_id

    dp = create_dataprovider(dataset=table, transfer_mode=transfer_mode)

    if not table.name:
        # We create a unique table name to make the name unique across runs
        table.name = create_unique_str(UNIQUE_HASH_SIZE)
    file = params.get("file")

    dp.populate_metadata()
    # dp.create_schema_if_needed(table.metadata.schema)

    if file:
        dp.load_file_to_table(file, table)
    yield dp, table
    dp.drop_table(table)


def set_table_missing_values(table: Table, dataset_name: str) -> Table:
    """
    Set missing values of table dataset
    """
    conn_id = DATASET_NAME_TO_CONN_ID[dataset_name]
    table = table or Table(conn_id=conn_id)

    if not table.conn_id:
        table.conn_id = conn_id

    if not table.name:
        # We create a unique table name to make the name unique across runs
        table.name = create_unique_str(UNIQUE_HASH_SIZE)

    return table


def set_file_missing_values(file: File, dataset_name: str):
    """
    Set missing values of file dataset
    """
    conn_id = DATASET_NAME_TO_CONN_ID[dataset_name]
    if not file.conn_id:
        file.conn_id = conn_id
    return file


def populate_file(src_file_path: str, dataset_provider: BaseFilesystemProviders, dp_name: str):
    """
    Populate file with local file data
    """
    src_file_object = dataset_provider._convert_remote_file_to_byte_stream(src_file_path)
    mode = "wb" if dataset_provider.read_as_binary(src_file_path) else "w"

    # Currently, we are passing the credentials to sftp server via URL - sftp://username:password@localhost, we are
    # populating the credentials in the URL if the server destination is SFTP.
    path = dataset_provider.dataset.path
    if dp_name == "SFTPDataProvider":
        original_url = urlparse(path)
        cred_url = urlparse(dataset_provider.get_uri())
        url_netloc = f"{cred_url.netloc}/{original_url.netloc}"
        url_path = original_url.path
        cred_url = cred_url._replace(netloc=url_netloc, path=url_path)
        path = urlunparse(cred_url)

    with smart_open.open(path, mode=mode, transport_params=dataset_provider.transport_params) as stream:
        stream.write(src_file_object.read())
        stream.flush()


def set_missing_values(dataset_object: [File, Table], dp_name: str) -> [File, Table]:
    """Set missing values for datasets"""
    dataset_type = DATASET_NAME_TO_PROVIDER_TYPE[dp_name]
    if dataset_type == "database":
        dataset_object = set_table_missing_values(table=dataset_object, dataset_name=dp_name)
    elif dataset_type == "file":
        dataset_object = set_file_missing_values(file=dataset_object, dataset_name=dp_name)
    return dataset_object


def load_data_in_datasets(
    dataset_object: [File, Table], dp: DataProviders, dp_name: str, local_file_path: str
):
    """
    Load data in datasets
    :param dataset_object: user passed Dataset object
    :param dp: DataProviders object created using dataset_object
    :param dp_name: name of data_provider class
    :param local_file_path: data that needs to be loaded in dataset
    """
    dataset_type = DATASET_NAME_TO_PROVIDER_TYPE[dp_name]
    if dataset_type == "database":
        dp.create_schema_if_needed(dataset_object.metadata.schema)
        if local_file_path:
            dp.load_file_to_table(File(local_file_path), dataset_object)
    elif dataset_type == "file":
        if local_file_path:
            populate_file(src_file_path=local_file_path, dataset_provider=dp, dp_name=dp_name)


def delete_dataset(dataset_object: [File, Table], dp: DataProviders, dp_name: str, local_file_path: str):
    """
    Delete dataset
    :param dataset_object: user passed Dataset object
    :param dp: DataProviders object created using dataset_object
    :param dp_name: name of data_provider class
    :param local_file_path: data that needs to be loaded in dataset
    """
    dataset_type = DATASET_NAME_TO_PROVIDER_TYPE[dp_name]
    if dataset_type == "database":
        dp.drop_table(dataset_object)
    elif dataset_type == "file" and local_file_path:
        try:
            dp.delete()
        except (FileNotFoundError, NotFound):
            pass


def dataset_fixture(request):
    """
    Fixture to populate data in datasets and fill in missing values. For file dataset we need to pass an "object"
     parameter with absolute path of the file.
    Example:
        @pytest.mark.parametrize(
        "src_dataset_fixture",
            [
                {"name": "SqliteDataProvider", "local_file_path": f"{str(CWD)}/../../data/sample.csv"},
                {
                    "name": "S3DataProvider",
                    "object": File(path=f"s3://tmp9/{create_unique_str(10)}.csv"),
                    "local_file_path": f"{str(CWD)}/../../data/sample.csv",
                }
            ],
            indirect=True,
            ids=lambda dp: dp["name"],
        )

    """
    # We deepcopy the request param dictionary as we modify the table item directly.
    params = deepcopy(request.param)

    dp_name = params["name"]
    dataset_object = params.get("object", None)
    transfer_mode = params.get("transfer_mode", TransferMode.NONNATIVE)
    local_file_path = params.get("local_file_path")

    dataset_object = set_missing_values(dataset_object=dataset_object, dp_name=dp_name)

    dp = create_dataprovider(dataset=dataset_object, transfer_mode=transfer_mode)
    dp.populate_metadata()

    load_data_in_datasets(
        dataset_object=dataset_object, dp=dp, dp_name=dp_name, local_file_path=local_file_path
    )

    yield dp, dataset_object

    delete_dataset(dataset_object=dataset_object, dp=dp, dp_name=dp_name, local_file_path=local_file_path)


@pytest.fixture
def src_dataset_fixture(request):
    yield from dataset_fixture(request)


@pytest.fixture
def dst_dataset_fixture(request):
    yield from dataset_fixture(request)
