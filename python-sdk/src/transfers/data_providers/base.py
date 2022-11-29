from __future__ import annotations

from abc import ABC
from typing import Any

from airflow.hooks.dbapi import DbApiHook
from transfers.constants import FileLocation
from transfers.datasets.base import UniversalDataset as Dataset
from transfers.utils import get_dataset_connection_type

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
        self.LOAD_DATA_FROM_SOURCE: dict = {}

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.conn_id})'

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    def check_if_exists(self, dataset: Dataset) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def check_if_transfer_supported(self, source_dataset: Dataset) -> bool:
        """
        Checks if the transfer is supported from source to destination based on source_dataset.
        """
        source_connection_type = get_dataset_connection_type(source_dataset)
        return FileLocation(source_connection_type) in self.transfer_mapping

    def load_data_from_source(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        """
        Loads data from source dataset to the destination using data provider
        """
        if not self.check_if_transfer_supported(source_dataset=source_dataset):
            raise ValueError("Transfer not supported yet.")
        source_connection_type = get_dataset_connection_type(source_dataset)
        method_name = self.LOAD_DATA_FROM_SOURCE.get(source_connection_type)
        if method_name:
            transfer_method = self.__getattribute__(method_name)
            return transfer_method(
                source_dataset=source_dataset,
                destination_dataset=destination_dataset,
            )
        else:
            raise ValueError(f"No transfer performed from {source_connection_type} to S3.")

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
