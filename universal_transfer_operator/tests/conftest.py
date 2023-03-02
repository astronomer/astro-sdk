import logging
import os
from copy import deepcopy

import pytest
import yaml
from airflow.models import DAG, Connection, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import create_session
from utils.test_utils import create_unique_str

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers import create_dataprovider
from universal_transfer_operator.datasets.table import Table

DEFAULT_DATE = timezone.datetime(2016, 1, 1)
UNIQUE_HASH_SIZE = 16

DATASET_NAME_TO_CONN_ID = {
    "SqliteDataProvider": "sqlite_default",
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

    dp.populate_table_metadata(table)
    dp.create_schema_if_needed(table.metadata.schema)

    if file:
        dp.load_file_to_table(file, table)
    yield dp, table
    dp.drop_table(table)
