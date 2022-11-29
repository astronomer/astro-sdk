from airflow.hooks.base import BaseHook
from transfers.datasets.base import UniversalDataset as Dataset


def check_if_connection_exists(conn_id: str) -> bool:
    """
    Given an Airflow connection ID, identify if it exists.
    Return True if it does or raise an AirflowNotFoundException exception if it does not.

    :param conn_id: Airflow connection ID
    :return bool: If the connection exists, return True
    """
    try:
        BaseHook.get_connection(conn_id)
    except ValueError:
        return False
    return True


def get_dataset_connection_type(dataset: Dataset) -> str:
    """
    Given dataset fetch the connection type based on airflow connection
    """
    return BaseHook.get_connection(dataset.conn_id).conn_type
