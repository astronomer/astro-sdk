import importlib
from enum import Enum

from airflow.hooks.base import BaseHook
from uto.data_providers.base import DataProviders
from uto.datasets.base import UniversalDataset as Dataset
from uto.integrations.base import TransferIntegrations

from astro.constants import LoadExistStrategy
from astro.utils.path import get_class_name

CUSTOM_INGESTION_TYPE_TO_MODULE_PATH = {"Fivetran": "uto.integrations.fivetran"}

DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING = {
    "s3": "uto.data_providers.aws.s3",
    "aws": "uto.data_providers.aws.s3",
    "gs": "uto.data_providers.google.cloud.gcs",
    "google_cloud_platform": "uto.data_providers.google.cloud.gcs",
}


class FileLocation(Enum):
    # [START filelocation]
    LOCAL = "local"
    HTTP = "http"
    HTTPS = "https"
    GS = "google_cloud_platform"  # Google Cloud Storage
    google_cloud_platform = "google_cloud_platform"  # Google Cloud Storage
    S3 = "s3"  # Amazon S3
    AWS = "aws"
    # [END filelocation]


class IngestorSupported(Enum):
    # [START transferingestor]
    Fivetran = "fivetran"
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


def create_dataprovider(
    dataset: Dataset,
    optimization_params: dict = {},
    extras: dict = {},
    use_optimized_transfer: bool = True,
    if_exists: LoadExistStrategy = "replace",
) -> DataProviders:
    from airflow.hooks.base import BaseHook

    conn_type = BaseHook.get_connection(dataset.conn_id).conn_type
    module_path = DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING[conn_type]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="DataProvider")
    data_provider: DataProviders = getattr(module, class_name)(
        conn_id=dataset.conn_id,
        optimization_params=optimization_params,
        extras=extras,
        use_optimized_transfer=use_optimized_transfer,
        if_exists=if_exists,
    )
    return data_provider


def create_transfer_integration(
    ingestion_type: IngestorSupported, ingestion_config: dict = {}
) -> TransferIntegrations:
    """
    Given a ingestion_type and ingestion_config return the associated TransferIntegrations class.

    :param ingestion_type: Use ingestion_type for methods involved in transfer using FiveTran.
    :param ingestion_config: kwargs to be used by methods involved in transfer using FiveTran.
    :return:
    """
    module_path = CUSTOM_INGESTION_TYPE_TO_MODULE_PATH[ingestion_type.name]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="Integration")
    transfer_integrations: TransferIntegrations = getattr(module, class_name)(ingestion_config)
    return transfer_integrations


def get_dataset_connection_type(dataset: Dataset) -> str:
    """
    Given dataset fetch the connection type based on airflow connection
    """
    from airflow.hooks.base import BaseHook

    return BaseHook.get_connection(dataset.conn_id).conn_type
