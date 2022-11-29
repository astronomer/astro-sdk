from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from airflow.hooks.dbapi import DbApiHook
from transfers.datasets.base import UniversalDataset as Dataset

from astro.constants import LoadExistStrategy


class DataProviders(ABC):
    """
    Base class to represent all the DataProviders interactions with Dataset.

    The goal is to be able to support new dataset by adding
    a new module to the `uto/data_providers` directory, without the need of
    changing other modules and classes.
    """

    def __init__(
        self,
        conn_id: str,
        transfer_mode,
        extra: dict = {},
        transfer_params: dict = {},
        if_exists: LoadExistStrategy = "replace",
    ):
        self.conn_id = conn_id
        self.extra = extra
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.if_exists = if_exists
        self.transfer_mapping: Any = {}

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.conn_id})'

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    def check_if_exists(self, dataset: Dataset) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    # def check_if_transfer_supported(self, source_dataset: Dataset) -> bool:
    #     """
    #     Checks if the transfer is supported from source to destination based on source_dataset.
    #     """
    #     source_connection_type = get_dataset_connection_type(source_dataset)
    #     return source_connection_type in self.transfer_mapping

    @abstractmethod
    def load_data_from_source(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
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
