import importlib

from airflow.hooks.base import BaseHook
from universal_transfer_operator.constants import LoadExistStrategy, TransferMode
from universal_transfer_operator.data_providers.base import DataProviders
from universal_transfer_operator.datasets.base import UniversalDataset as Dataset
from universal_transfer_operator.utils import get_class_name

DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING = {
    "s3": "universal_transfer_operator.data_providers.filesystem.aws.s3",
    "aws": "universal_transfer_operator.data_providers.filesystem.aws.s3",
    "gs": "universal_transfer_operator.data_providers.filesystem.google.cloud.gcs",
    "google_cloud_platform": "universal_transfer_operator.data_providers.filesystem.google.cloud.gcs",
}


def create_dataprovider(
    dataset: Dataset,
    transfer_params: dict = None,
    transfer_mode: TransferMode = TransferMode.NONNATIVE,
    if_exists: LoadExistStrategy = "replace",
) -> DataProviders:
    conn_type = BaseHook.get_connection(dataset.conn_id).conn_type
    module_path = DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING[conn_type]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="DataProvider")
    data_provider: DataProviders = getattr(module, class_name)(
        dataset=dataset,
        transfer_params=transfer_params,
        transfer_mode=transfer_mode,
        if_exists=if_exists,
    )
    return data_provider
