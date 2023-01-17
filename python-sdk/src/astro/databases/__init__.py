from __future__ import annotations

import importlib
from pathlib import Path
from typing import TYPE_CHECKING

from astro.options import LoadOptionsList
from astro.utils.compat.functools import cache
from astro.utils.path import get_class_name, get_dict_with_module_names_to_dot_notations

if TYPE_CHECKING:  # pragma: no cover
    from astro.databases.base import BaseDatabase
    from astro.table import BaseTable

DEFAULT_CONN_TYPE_TO_MODULE_PATH = get_dict_with_module_names_to_dot_notations(Path(__file__))
CUSTOM_CONN_TYPE_TO_MODULE_PATH = {
    "gcpbigquery": DEFAULT_CONN_TYPE_TO_MODULE_PATH["bigquery"],
    "google_cloud_platform": DEFAULT_CONN_TYPE_TO_MODULE_PATH["bigquery"],
    "databricks": "astro.databases.databricks.delta",
    "delta": "astro.databases.databricks.delta",
}
CONN_TYPE_TO_MODULE_PATH = {
    **DEFAULT_CONN_TYPE_TO_MODULE_PATH,
    **CUSTOM_CONN_TYPE_TO_MODULE_PATH,
}
SUPPORTED_DATABASES = set(DEFAULT_CONN_TYPE_TO_MODULE_PATH.keys())


def create_database(
    conn_id: str, table: BaseTable | None = None, load_options_list: LoadOptionsList | None = None
) -> BaseDatabase:
    """
    Given a conn_id, return the associated Database class.

    :param conn_id: Database connection ID in Airflow
    :param table: (optional) The Table object
    """
    module = importlib.import_module(_get_conn(conn_id))
    class_name = get_class_name(module_ref=module, suffix="Database")
    database_class = getattr(module, class_name)
    load_options = load_options_list and load_options_list.get(database_class)
    database: BaseDatabase = database_class(conn_id, table, load_options=load_options)
    return database


@cache
def _get_conn(conn_id: str) -> str:
    from airflow.hooks.base import BaseHook

    conn_type = BaseHook.get_connection(conn_id).conn_type
    module_path = CONN_TYPE_TO_MODULE_PATH[conn_type]
    return module_path
