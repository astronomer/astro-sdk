from __future__ import annotations

import io
from abc import ABC
from pathlib import Path
from typing import Generic, Iterator, TypeVar

import attr
import pandas as pd
from airflow.hooks.base import BaseHook

from universal_transfer_operator.constants import Location
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Table
from universal_transfer_operator.utils import TransferParameters, get_dataset_connection_type

DatasetType = TypeVar("DatasetType", File, Table)


@attr.define
class DataStream:
    remote_obj_buffer: io.IOBase
    actual_filename: Path
    actual_file: File


class DataProviders(ABC, Generic[DatasetType]):
    """
    Base class to represent all the DataProviders interactions with Dataset.

    The goal is to be able to support new dataset by adding
    a new module to the `uto/data_providers` directory, without the need of
    changing other modules and classes.
    """

    def __init__(
        self,
        dataset: DatasetType,
        transfer_mode,
        transfer_params: TransferParameters = attr.field(
            factory=TransferParameters,
            converter=lambda val: TransferParameters(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.dataset: DatasetType = dataset
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping: set[Location] = set()
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

    @property
    def hook(self) -> BaseHook:
        """Return an instance of the Airflow hook."""
        raise NotImplementedError

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def check_if_transfer_supported(self, source_dataset: DatasetType) -> bool:
        """
        Checks if the transfer is supported from source to destination based on source_dataset.
        """
        source_connection_type = get_dataset_connection_type(source_dataset)
        return Location(source_connection_type) in self.transfer_mapping

    def read(self) -> Iterator[pd.DataFrame] | Iterator[DataStream]:
        """Read from filesystem dataset or databases dataset and write to local reference locations or dataframes"""
        raise NotImplementedError

    def write(self, source_ref: pd.DataFrame | DataStream) -> str:  # type: ignore
        """Write the data from local reference location or a dataframe to the database dataset or filesystem dataset

        :param source_ref: Stream of data to be loaded into output table or a pandas dataframe.
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

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace}{self.openlineage_dataset_name}"

    def populate_metadata(self):
        """
        Given a dataset, check if the dataset has metadata.
        """
        raise NotImplementedError
