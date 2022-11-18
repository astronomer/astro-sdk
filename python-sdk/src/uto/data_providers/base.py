from __future__ import annotations

from abc import ABC, abstractmethod

from airflow.hooks.dbapi import DbApiHook
from uto.datasets.base import UniversalDataset as Dataset


class DataProviders(ABC):
    """
    Base class to represent all the DataProviders interactions with Dataset.

    The goal is to be able to support new dataset by adding
    a new module to the `uto/data_providers` directory, without the need of
    changing other modules and classes.
    """

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.conn_id})'

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
