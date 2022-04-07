from .bigquery import BigQuery
from .sqlite import SQLite


def get_db_from_conn_type(conn_type):
    conn_type_to_db = {
        "bigquery": BigQuery,
        "gcpbigquery": BigQuery,
        "google_cloud_platform": BigQuery,
        "sqlite": SQLite,
        # "postgres": Database.POSTGRES,
        # "postgresql": Database.POSTGRES,
        # "snowflake": Database.SNOWFLAKE,
    }
    return conn_type_to_db.get(conn_type)
