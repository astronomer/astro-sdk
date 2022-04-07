from ..table import Table
from .bigquery import BigQuery
from .sqlite import Sqlite

conn_type_to_db = {
    "bigquery": BigQuery,
    "gcpbigquery": BigQuery,
    "google_cloud_platform": BigQuery,
    "sqlite": Sqlite,
    # "postgres": Database.POSTGRES,
    # "postgresql": Database.POSTGRES,
    # "snowflake": Database.SNOWFLAKE,
}


def get_db_from_conn_type(conn_type: str):
    return conn_type_to_db.get(conn_type)


def get_db_from_table(table: Table):
    return get_db_from_conn_type(table.conn_type)(table)
