import importlib
from enum import Enum
from pathlib import Path

from airflow.hooks.base import BaseHook
from uto.data_providers.base import DataProviders
from uto.datasets.base import UniversalDataset as Dataset
from uto.integrations.base import TransferIntegrations

from astro.utils.path import get_class_name, get_dict_with_module_names_to_dot_notations

DEFAULT_INGESTION_TYPE_TO_MODULE_PATH = get_dict_with_module_names_to_dot_notations(Path(__file__))
CUSTOM_INGESTION_TYPE_TO_MODULE_PATH = {
    "fivetran": DEFAULT_INGESTION_TYPE_TO_MODULE_PATH["fivetran"],
}

INGESTION_TYPE_TO_MODULE_PATH = {**DEFAULT_INGESTION_TYPE_TO_MODULE_PATH}

DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING = {"s3": "uto.data_providers.aws.s3.S3DataProviders"}


class IngestorSupported(Enum):
    # [START transferingestor]
    FiveTran = "fivetran"
    # [END transferingestor]


def check_if_connection_exists(conn_id: str) -> bool:
    """
    Given an Airflow connection ID, identify if it exists.
    Return True if it does or raise an AirflowNotFoundException exception if it does not.

    :param conn_id: Airflow connection ID
    :return bool: If the connection exists, return True
    """
    BaseHook.get_connection(conn_id)
    return True


def create_dataprovider(dataset: Dataset) -> DataProviders:
    from airflow.hooks.base import BaseHook

    conn_type = BaseHook.get_connection(dataset.conn_id).conn_type
    module_path = DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING[conn_type]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="DataProvider")
    data_provider: DataProviders = getattr(module, class_name)(dataset.conn_id)
    return data_provider


def create_transfer_integration(
    ingestion_type: IngestorSupported, ingestion_config: dict | None
) -> TransferIntegrations:
    module_path = INGESTION_TYPE_TO_MODULE_PATH[ingestion_type.name]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="Integration")
    transfer_integrations: TransferIntegrations = getattr(module, class_name)(ingestion_config)
    return transfer_integrations
