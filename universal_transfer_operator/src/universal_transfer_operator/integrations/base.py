from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.hooks.dbapi import DbApiHook

from universal_transfer_operator.datasets.base import UniversalDataset as Dataset


class TransferIntegration(ABC):
    """
    Class for third party transfer.

    """

    def __init__(self, conn_id: str, transfer_params: dict):
        self.conn_id = conn_id
        self.transfer_params = transfer_params
        # transfer mapping creates a mapping between various sources and destination, where
        # transfer is possible using the integration
        self.transfer_mapping: dict[str, str] = None
        # TODO: add method for validation, transfer mapping, transfer params etc

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    @abstractmethod
    def transfer_job(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        """
        Loads data from source dataset to the destination using ingestion config
        """
        raise NotImplementedError
