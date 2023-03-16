from __future__ import annotations

import io
import os
from abc import abstractmethod
from pathlib import Path
from typing import Any

import attr
import smart_open
from airflow.hooks.base import BaseHook

from universal_transfer_operator.constants import FileType, Location
from universal_transfer_operator.data_providers.base import DataProviders
from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.file.types import create_file_type
from universal_transfer_operator.universal_transfer_operator import TransferIntegrationOptions
from universal_transfer_operator.utils import get_dataset_connection_type


@attr.define
class TempFile:
    tmp_file: Path | None
    actual_filename: Path


@attr.define
class FileStream:
    remote_obj_buffer: io.IOBase
    actual_filename: Path
    actual_file: File


class BaseFilesystemProviders(DataProviders[File]):
    """BaseFilesystemProviders represent all the DataProviders interactions with File system."""

    def __init__(
        self,
        dataset: File,
        transfer_mode,
        transfer_params: TransferIntegrationOptions = attr.field(
            factory=TransferIntegrationOptions,
            converter=lambda val: TransferIntegrationOptions(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.dataset = dataset
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping = set()
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}
        super().__init__(
            dataset=self.dataset, transfer_mode=self.transfer_mode, transfer_params=self.transfer_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

    @property
    def hook(self) -> BaseHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    @property
    @abstractmethod
    def paths(self) -> list[str]:
        """Resolve patterns in path"""
        raise NotImplementedError

    def delete(self):
        """
        Delete a file/object if they exists
        """
        raise NotImplementedError

    @property
    def transport_params(self) -> dict | None:  # skipcq: PYL-R0201
        """Get credentials required by smart open to access files"""
        return None

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def exists(self) -> bool:
        return self.check_if_exists()

    def check_if_transfer_supported(self, source_dataset: File) -> bool:
        """
        Checks if the transfer is supported from source to destination based on source_dataset.
        """
        source_connection_type = get_dataset_connection_type(source_dataset)
        return Location(source_connection_type) in self.transfer_mapping

    def read(self):
        """ ""Read the dataset and write to local reference location"""
        return self.read_using_smart_open()

    def read_using_smart_open(self):
        """Read the file dataset using smart open returns i/o buffer"""
        files = self.paths
        for file in files:
            yield FileStream(
                remote_obj_buffer=self._convert_remote_file_to_byte_stream(file),
                actual_filename=file,
                actual_file=self.dataset,
            )

    def _convert_remote_file_to_byte_stream(self, file: str) -> io.IOBase:
        """
        Read file from all supported location and convert them into a buffer that can be streamed into other data
        structures.

        :returns: an io object that can be streamed into a dataframe (or other object)
        """
        mode = "rb" if self.read_as_binary(file) else "r"
        remote_obj_buffer = io.BytesIO() if self.read_as_binary(file) else io.StringIO()
        with smart_open.open(file, mode=mode, transport_params=self.transport_params) as stream:
            remote_obj_buffer.write(stream.read())
            remote_obj_buffer.seek(0)
            return remote_obj_buffer

    def write(self, source_ref: FileStream):
        """
        Write the data from local reference location to the dataset
        :param source_ref: Source FileStream object which will be used to read data
        """
        return self.write_using_smart_open(source_ref=source_ref)

    def write_using_smart_open(self, source_ref: FileStream):
        """Write the source data from remote object i/o buffer to the dataset using smart open"""
        mode = "wb" if self.read_as_binary(source_ref.actual_file.path) else "w"
        destination_file = self.dataset.path
        with smart_open.open(destination_file, mode=mode, transport_params=self.transport_params) as stream:
            stream.write(source_ref.remote_obj_buffer.read())
        return destination_file

    def read_as_binary(self, file: str) -> bool:
        """
        Checks if file has to be read as binary or as string i/o.

        :return: True or False
        """
        try:
            filetype = create_file_type(
                path=file,
                filetype=self.dataset.filetype,
                normalize_config=self.dataset.normalize_config,
            )
        except ValueError:
            # return True when the extension of file is not supported
            # Such file can be read as binary and transferred.
            return True

        read_as_non_binary = {FileType.CSV, FileType.JSON, FileType.NDJSON}
        if filetype in read_as_non_binary:
            return False
        return True

    @staticmethod
    def cleanup(file_list: list[TempFile]) -> None:
        """Cleans up the temporary files created"""
        for file in file_list:
            if os.path.exists(file.actual_filename):
                os.remove(file.actual_filename)

    def load_data_from_source_natively(self, source_dataset: File, destination_dataset: Dataset) -> Any:
        """
        Loads data from source dataset to the destination using data provider
        """
        if not self.check_if_transfer_supported(source_dataset=source_dataset):
            raise ValueError("Transfer not supported yet.")

        source_connection_type = get_dataset_connection_type(source_dataset)
        method_name = self.LOAD_DATA_NATIVELY_FROM_SOURCE.get(source_connection_type)
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

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def size(self) -> int:
        """Return the size in bytes of the given file"""
        raise NotImplementedError

    def populate_metadata(self):  # skipcq: PTC-W0049
        """
        Given a dataset, check if the dataset has metadata.
        """
        pass
