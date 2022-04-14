import importlib

from airflow.hooks.base import BaseHook

BIGQUERY_MODULE_PATH = "astro.databases.google.bigquery"

CONN_TYPE_TO_MODULE_PATH = {
    "bigquery": BIGQUERY_MODULE_PATH,
    "gcpbigquery": BIGQUERY_MODULE_PATH,
    "google_cloud_platform": BIGQUERY_MODULE_PATH,
    "sqlite": "astro.databases.sqlite",
}


def get_database_from_conn_id(conn_id):
    conn_type = BaseHook.get_connection(conn_id).conn_type
    module_path = CONN_TYPE_TO_MODULE_PATH[conn_type]
    module = importlib.import_module(module_path)
    return module.Database(conn_id)
