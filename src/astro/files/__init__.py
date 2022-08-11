from airflow.decorators.base import TaskDecorator, get_unique_task_id
from airflow.hooks.base import BaseHook

from astro.files.base import File  # noqa: F401 # skipcq: PY-W2000
from astro.files.base import resolve_file_path_pattern  # noqa: F401 # skipcq: PY-W2000
from astro.files.operators.files import _ListFileOperator


def check_if_connection_exists(conn_id: str) -> bool:
    """
    Given an Airflow connection ID, identify if it exists.
    Return True if it does or raise an AirflowNotFoundException exception if it does not.

    :param conn_id: Airflow connection ID
    :return bool: If the connection exists, return True
    """
    BaseHook.get_connection(conn_id)
    return True


def get_file_list(path: str, conn_id: str, **kwargs) -> TaskDecorator:
    """
    List the file path from the filesystem storage based on given path pattern

    Supported filesystem: Local, HTTP, S3, GCS

    :param conn_id: Airflow connection id
    :param path: Path pattern to the file in the filesystem/Object stores
    """
    if "task_id" in kwargs:
        task_id = kwargs["task_id"]
        del kwargs["task_id"]
    else:
        task_id = get_unique_task_id(task_id="get_file_list")

    return _ListFileOperator(
        task_id=task_id,
        path=path,
        conn_id=conn_id,
        **kwargs,
    ).output
