import importlib

from airflow.hooks.base import BaseHook

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.data_providers.base import DataProviders
from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.utils import TransferParameters, get_class_name

DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING = {
    ("s3", File): "universal_transfer_operator.data_providers.filesystem.aws.s3",
    ("aws", File): "universal_transfer_operator.data_providers.filesystem.aws.s3",
    ("gs", File): "universal_transfer_operator.data_providers.filesystem.google.cloud.gcs",
    ("google_cloud_platform", File): "universal_transfer_operator.data_providers.filesystem.google.cloud.gcs",
    ("sqlite", Table): "universal_transfer_operator.data_providers.database.sqlite",
    ("sftp", File): "universal_transfer_operator.data_providers.filesystem.sftp",
    ("sqlite", Table):  "universal_transfer_operator.data_providers.database.sqlite"
}


def create_dataprovider(
    dataset: Dataset,
    transfer_params: TransferParameters = None,
    transfer_mode: TransferMode = TransferMode.NONNATIVE,
) -> DataProviders:
    conn_type = BaseHook.get_connection(dataset.conn_id).conn_type
    module_path = DATASET_CONN_ID_TO_DATAPROVIDER_MAPPING[(conn_type, type(dataset))]
    module = importlib.import_module(module_path)
    class_name = get_class_name(module_ref=module, suffix="DataProvider")
    data_provider: DataProviders = getattr(module, class_name)(
        dataset=dataset,
        transfer_params=transfer_params,
        transfer_mode=transfer_mode,
    )
    return data_provider
