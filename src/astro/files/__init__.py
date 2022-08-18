from airflow.hooks.base import BaseHook
from airflow.models.xcom_arg import XComArg

from astro.files.base import File  # noqa: F401 # skipcq: PY-W2000
from astro.files.base import resolve_file_path_pattern  # noqa: F401 # skipcq: PY-W2000
from astro.files.operators.files import ListFileOperator


def check_if_connection_exists(conn_id: str) -> bool:
    """
    Given an Airflow connection ID, identify if it exists.
    Return True if it does or raise an AirflowNotFoundException exception if it does not.

    :param conn_id: Airflow connection ID
    :return bool: If the connection exists, return True
    """
    BaseHook.get_connection(conn_id)
    return True


def get_file_list(path: str, conn_id: str, **kwargs) -> XComArg:
    """
    List the file path from the filesystem storage based on given path pattern

    Supported filesystem: :ref:`file_location`

    Supported file pattern: :ref:`file_pattern`

    :param conn_id: Airflow connection id
    :param path: Path pattern to the file in the filesystem/Object stores
    """
    return ListFileOperator(
        path=path,
        conn_id=conn_id,
        **kwargs,
    ).output
