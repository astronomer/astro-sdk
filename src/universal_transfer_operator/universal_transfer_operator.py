from typing import TYPE_CHECKING, Any, Dict, Optional

from airflow.models import BaseOperator

from src.universal_transfer_operator.data_providers.base import DataProviders
from src.universal_transfer_operator.data_transfer.base import DataTransfer
from src.universal_transfer_operator.dataset import Dataset

if TYPE_CHECKING:
    from airflow.utils.context import Context


class UniversalTransferOperator(BaseOperator):
    """
    From a DAG author standpoint, all transfers would be performed through the invocation of
    only the Universal Transfer Operator.
    The source could for example be a file on S3 / GCS/ local etc and a destination for example
    could be a database table on Postgres /Snowflake / Redshift  etc. Or, the other way around
    for transferring data from Database tables into files. This enables the DAG author to work
    with a single interface regardlessof the source or destination data systems.

    There could be optional parameters specified for optimization, such as “optimize for memory usage”
    vs. “optimize for speed” and so on. An example is something like batch size which could be a useful
    tradeoff option for larger datasets.

     :param source_dataset: The targeted dataset source. This is the source Dataset object to transfer
        from.
    :param destination_dataset: The targeted dataset destination. This is the specified destination
        Dataset object to transfer to
    :param optimization_params: Dict of key-value pairs to optimize the transfers
        For example:
        optimization_params={
            "fast_transfer": False,
            "chunk_size": str(1024*5), # in bytes
        }
    """

    def __init__(
        self,
        *,
        source_dataset: Dataset,
        destination_dataset: Dataset,
        optimization_params: Optional[Dict],
        **kwargs
    ) -> None:
        super().__init__(**kwargs)
        self.source_dataset = source_dataset
        self.destination_dataset = destination_dataset
        self.optimization_params = optimization_params

    def execute(self, context: "Context") -> None:
        """
        TODO:
        1. Parse and validate the Dataset based on URI and conn_id.
        2. Get the source DataProviders and destination DataProvider.
        3. Fetch the dataset transfer type (filetofile, filetodb etc)
        4. Read the dataset from source using source DataProvider.
        4. Validate the dataset if applicable (check encoding).
        5. Transform the dataset if applicable.
        6. Transfer data to destination in desired format using
         destination DataProvider using DataTransfer.
        """
        pass

    def get_transfer_type(
        self,
        source_data_provider: DataProviders,
        destination_data_provider: DataProviders,
    ) -> DataTransfer:
        # TODO: Add logic to get the DataTransfer type
        pass

    def validate_dataset(self, dataset: Dataset) -> Any:
        # TODO: add logic to validate the data (eg. encoding etc)
        pass

    def transform_dataset(self) -> Any:
        # TODO : Transform the dataset if applicable.
        pass

    def transfer_dataset(
        self,
        source_dataset: Dataset,
        destination_dataset: Dataset,
        source_data_provider: DataProviders,
        destination_data_provider: DataProviders,
    ) -> None:
        # TODO: Transfer data to destination in desired format using destination_provider.
        pass

    def _get_provider(self, dataset: Dataset) -> DataProviders:
        """
        Given the dataset, fetch the DataProviders from the connection and uri
        """
        return dataset.get_provider()
