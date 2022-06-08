from airflow.hooks.base import BaseHook

from astro.files.base import File, get_files  # noqa: F401 # skipcq: PY-W2000


def check_if_connection_exists(conn_id: str) -> bool:
    """
    Given an Airflow connection ID, identify if it exists.
    Return True if it does or raise an AirflowNotFoundException exception if it does not.

    :param conn_id: Airflow connection ID
    :return bool: If the connection exists, return True
    """
    BaseHook.get_connection(conn_id)
    return True
