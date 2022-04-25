"""

"""
import importlib
from pathlib import Path

from airflow.hooks.base import BaseHook

from astro.databases.base import BaseDatabase
from astro.utils.path import get_dict_with_module_names_to_dot_notations

DEFAULT_CONN_TYPE_TO_MODULE_PATH = get_dict_with_module_names_to_dot_notations(
    Path(__file__)
)
CUSTOM_CONN_TYPE_TO_MODULE_PATH = {
    "gcpbigquery": DEFAULT_CONN_TYPE_TO_MODULE_PATH["bigquery"],
    "google_cloud_platform": DEFAULT_CONN_TYPE_TO_MODULE_PATH["bigquery"],
}
CONN_TYPE_TO_MODULE_PATH = {
    **DEFAULT_CONN_TYPE_TO_MODULE_PATH,
    **CUSTOM_CONN_TYPE_TO_MODULE_PATH,
}
SUPPORTED_DATABASES = set(DEFAULT_CONN_TYPE_TO_MODULE_PATH.keys())


def get_database_from_conn_id(conn_id: str = "") -> BaseDatabase:
    conn_type = BaseHook.get_connection(conn_id).conn_type
    module_path = CONN_TYPE_TO_MODULE_PATH[conn_type]
    module = importlib.import_module(module_path)
    module_name = module_path.split(".")[-1]
    class_name = module_name.title() + "Database"
    return getattr(module, class_name)(conn_id)
