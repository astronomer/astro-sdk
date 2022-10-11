from typing import TYPE_CHECKING

from airflow.hooks.base import BaseHook

from astro.files.base import File  # noqa: F401 # skipcq: PY-W2000
from astro.files.base import resolve_file_path_pattern  # noqa: F401 # skipcq: PY-W2000

if TYPE_CHECKING:
    from airflow.models.xcom_arg import XComArg


def check_if_connection_exists(conn_id: str) -> bool:
    """
    Given an Airflow connection ID, identify if it exists.
    Return True if it does or raise an AirflowNotFoundException exception if it does not.

    :param conn_id: Airflow connection ID
    :return bool: If the connection exists, return True
    """
    BaseHook.get_connection(conn_id)
    return True


def get_file_list(path: str, conn_id: str, **kwargs) -> "XComArg":
    """
    List file path from a remote object store or the local filesystem based on the given path pattern.
    It is not advisable to fetch huge number of files since it would overload the XCOM and
    also, if you are using response of this method in dynamic task map ``expand`` method
    it can create huge number of parallel task

    Supported filesystem: :ref:`file_location`

    Supported file pattern: :ref:`file_pattern`

    :param conn_id: Airflow connection id.
        This will be used to identify the right Airflow hook at runtime to connect with storage services
    :param path: Path pattern to the file in the filesystem/Object stores
    """
    from astro.files.operators.files import ListFileOperator

    return ListFileOperator(
        path=path,
        conn_id=conn_id,
        **kwargs,
    ).output
