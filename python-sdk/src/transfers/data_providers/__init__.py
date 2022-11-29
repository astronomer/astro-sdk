import importlib

from transfers.constants import TransferMode
from transfers.data_providers.base import DataProviders
from transfers.datasets.base import UniversalDataset as Dataset

from astro.constants import LoadExistStrategy
from astro.utils.path import get_class_name

DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING = {
    "s3": "transfers.data_providers.aws.s3",
    "aws": "transfers.data_providers.aws.s3",
    "gs": "transfers.data_providers.google.cloud.gcs",
    "google_cloud_platform": "transfers.data_providers.google.cloud.gcs",
}


def create_dataprovider(
    dataset: Dataset,
    transfer_params: dict = {},
    transfer_mode: TransferMode = TransferMode.NONNATIVE,
    if_exists: LoadExistStrategy = "replace",
) -> DataProviders:
    from airflow.hooks.base import BaseHook

    conn_type = BaseHook.get_connection(dataset.conn_id).conn_type
    module_path = DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING[conn_type]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="DataProvider")
    data_provider: DataProviders = getattr(module, class_name)(
        conn_id=dataset.conn_id,
        extra=dataset.extra,
        transfer_params=transfer_params,
        transfer_mode=transfer_mode,
        if_exists=if_exists,
    )
    return data_provider
