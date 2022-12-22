from __future__ import annotations

from typing import Any

from airflow.models import BaseOperator
from airflow.utils.context import Context
from transfers.constants import TransferMode
from transfers.data_providers import create_dataprovider
from transfers.datasets.base import UniversalDataset as Dataset
from transfers.integrations import get_transfer_integration
from transfers.utils import check_if_connection_exists

from astro.constants import LoadExistStrategy


class UniversalTransferOperator(BaseOperator):
    """
    Transfers all the data that could be read from the source Dataset into the destination Dataset. From a DAG author
    standpoint, all transfers would be performed through the invocation of only the Universal Transfer Operator.

    :param source_dataset: Source dataset to be transferred.
    :param destination_dataset: Destination dataset to be transferred to.
    :param transfer_params: kwargs to be used by method involved in transfer flow.
    :param transfer_mode: Use transfer_mode TransferMode; native, non-native or thirdparty.
    :param if_exists: Overwrite file if exists. Default False.

    :return: returns the destination dataset
    """

    def __init__(
        self,
        *,
        source_dataset: Dataset,
        destination_dataset: Dataset,
        transfer_params: dict | None = {},
        transfer_mode: TransferMode | None = TransferMode.NONNATIVE,
        if_exists: LoadExistStrategy = "replace",
        **kwargs,
    ) -> None:
        self.source_dataset = source_dataset
        self.destination_dataset = destination_dataset
        self.transfer_mode = transfer_mode or TransferMode.NONNATIVE
        self.transfer_params = transfer_params or {}
        self.if_exists = if_exists
        super().__init__(**kwargs)

    def execute(self, context: Context) -> Any:
        if self.source_dataset.conn_id:
            if not check_if_connection_exists(self.source_dataset.conn_id):
                raise ValueError("source_dataset connection does not exist.")

        if self.destination_dataset.conn_id:
            if not check_if_connection_exists(self.source_dataset.conn_id):
                raise ValueError("destination_dataset connection does not exist.")

        if self.transfer_mode is TransferMode.THIRDPARTY:
            transfer_integration = get_transfer_integration(self.transfer_params)
            return transfer_integration.transfer_job(self.source_dataset, self.destination_dataset)

        destination_dataprovider = create_dataprovider(
            dataset=self.destination_dataset,
            transfer_params=self.transfer_params,
            transfer_mode=self.transfer_mode,
            if_exists=self.if_exists,
        )
        return destination_dataprovider.load_data_from_source(self.source_dataset, self.destination_dataset)
