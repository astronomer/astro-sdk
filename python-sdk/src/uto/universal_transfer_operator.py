from __future__ import annotations

from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context
from uto.datasets.base import UniversalDataset as Dataset
from uto.utils import (
    IngestorSupported,
    check_if_connection_exists,
    create_dataprovider,
    create_transfer_integration,
)

from astro.constants import LoadExistStrategy


class UniversalTransferOperator(BaseOperator):
    """
    Transfers all the data that could be read from the source Dataset into the destination Dataset. From a DAG author
    standpoint, all transfers would be performed through the invocation of only the Universal Transfer Operator.

    :param source_dataset: Source dataset to be transferred.
    :param destination_dataset: Destination dataset to be transferred to.
    :param use_optimized_transfer: Use use_optimized_transfer for data transfer if available on the destination.
    :param optimization_params: kwargs to be used by method involved in optimized transfer flow.
    :param ingestion_config: kwargs to be used by methods involved in transfer using FiveTran.
    :param if_exists: Overwrite file if exists. Default False.

    :return: returns the destination dataset
    """

    def __init__(
        self,
        *,
        source_dataset: Dataset,
        destination_dataset: Dataset,
        use_optimized_transfer: bool = True,
        optimization_params: dict | None,
        ingestion_type: IngestorSupported | None = None,
        ingestion_config: dict | None = None,
        if_exists: LoadExistStrategy = "replace",
        **kwargs,
    ) -> None:

        super().__init__(**kwargs)
        self.source_dataset = source_dataset
        self.destination_dataset = destination_dataset
        self.ingestion_type = ingestion_type
        self.use_optimized_transfer = use_optimized_transfer
        self.optimization_params = optimization_params
        self.if_exists = if_exists
        self.ingestion_config = ingestion_config

    def execute(self, context: Context) -> Any:
        if self.source_dataset.conn_id:
            check_if_connection_exists(self.source_dataset.conn_id)

        if self.destination_dataset.conn_id:
            check_if_connection_exists(self.source_dataset.conn_id)

        if self.ingestion_type:
            transfer_integration = create_transfer_integration(self.ingestion_type, self.ingestion_config)
            return transfer_integration.transfer_job(self.source_dataset, self.destination_dataset)

        destination_dataprovider = create_dataprovider(self.destination_dataset)
        return destination_dataprovider.load_data_from_source(self.source_dataset)
