from __future__ import annotations

from abc import abstractmethod

from airflow.hooks.dbapi import DbApiHook
from uto.data_providers.base import DataProviders
from uto.datasets.base import UniversalDataset as Dataset


class S3DataProviders(DataProviders):
    """
    DataProviders interactions with S3 Dataset.
    """

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    def check_if_exists(self, dataset: Dataset) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    @abstractmethod
    def load_data_from_source(self, source_dataset: Dataset) -> None:
        """
        Loads data from source dataset to the destination using data provider
        """
        raise NotImplementedError

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError
