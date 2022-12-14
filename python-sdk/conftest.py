import random
import string
from copy import deepcopy

import pytest
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session

from astro.constants import Database
from astro.databases import create_database
from astro.table import MAX_TABLE_NAME_LENGTH, Table, TempTable

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

UNIQUE_HASH_SIZE = 16

DATABASE_NAME_TO_CONN_ID = {
    Database.SQLITE: "sqlite_default",
    Database.BIGQUERY: "google_cloud_default",
    Database.POSTGRES: "postgres_conn",
    Database.SNOWFLAKE: "snowflake_conn",
    Database.REDSHIFT: "redshift_conn",
    Database.DELTA: "databricks_conn",
    Database.MSSQL: "mssql_conn",
}


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
    # We deepcopy the request param dictionary as we modify the table item directly.
    params = deepcopy(request.param)

    database_name = params["database"]
    user_table = params.get("table", None)
    conn_id = DATABASE_NAME_TO_CONN_ID[database_name]
    if user_table and user_table.conn_id:
        conn_id = user_table.conn_id

    database = create_database(conn_id)
    table = params.get("table", Table(conn_id=database.conn_id, metadata=database.default_metadata))
    if not isinstance(table, TempTable):
        # We create a unique table name to make the name unique across runs
        table.name = create_unique_table_name(UNIQUE_HASH_SIZE)
    file = params.get("file")

    database.populate_table_metadata(table)
    database.create_schema_if_needed(table.metadata.schema)

    if file:
        database.load_file_to_table(file, table)
    yield database, table
    database.drop_table(table)
