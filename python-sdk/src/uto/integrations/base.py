from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.hooks.dbapi import DbApiHook
from uto.datasets.base import UniversalDataset as Dataset


class TransferIntegrations(ABC):
    """
    Class for third party transfer.

    """

    def __init__(self, ingestion_config: dict):
        self.ingestion_config = ingestion_config
        self.transfer_mapping: dict[str, str] = {}

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
