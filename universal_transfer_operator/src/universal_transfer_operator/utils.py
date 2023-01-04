from typing import Any

from airflow.hooks.base import BaseHook

from universal_transfer_operator.datasets.base import UniversalDataset as Dataset


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


def get_class_name(module_ref: Any, suffix: str = "Location") -> str:
    """Get class name to be dynamically imported. Class name are expected to be in following formats
    example -
    module name: test
    suffix: Abc

    expected class names -
        1. TESTAbc
        2. TestAbc

    :param module_ref: Module from which to get class location type implementation
    :param suffix: suffix for class name
    """
    module_name = module_ref.__name__.split(".")[-1]
    class_names_formats = [
        f"{module_name.title()}{suffix}",
        f"{module_name.upper()}{suffix}",
    ]
    for class_names_format in class_names_formats:
        if hasattr(module_ref, class_names_format):
            return class_names_format

    raise ValueError(
        "No expected class name found, please note that the class names should an expected formats."
    )
