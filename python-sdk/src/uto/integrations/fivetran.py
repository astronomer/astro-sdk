from __future__ import annotations

from airflow.hooks.dbapi import DbApiHook
from uto.datasets.base import UniversalDataset as Dataset
from uto.integrations.base import TransferIntegrations


class FivetranIntegration(TransferIntegrations):
    """
    Class for FiveTran.

    """

    def __init__(self, ingestion_config: dict):
        self.ingestion_config = ingestion_config
        self.transfer_mapping = {}

    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    def transfer_job(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        """
        Loads data from source dataset to the destination using ingestion config
        """
        raise NotImplementedError
