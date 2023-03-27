from __future__ import annotations

import glob
import os
from abc import ABC, abstractmethod
from pathlib import Path
from urllib.parse import urlparse

import smart_open
from airflow.hooks.base import BaseHook

from astro.constants import FileLocation
from astro.exceptions import DatabaseCustomError
from astro.options import LoadOptions


class BaseFileLocation(ABC):
    """Base Location abstract class"""

    template_fields = ("path", "conn_id")
    supported_conn_type: set[str] = set()

    def __init__(self, path: str, conn_id: str | None = None, load_options: LoadOptions | None = None):
        """
        Manages and provide interface for the operation for all the supported locations.

        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        self.path: str = path
        self.conn_id: str | None = conn_id
        self.load_options: LoadOptions | None = load_options
        self.validate_conn()

    def validate_conn(self):
        """Check if the conn_id matches with provided path."""
        if not self.conn_id:
            return

        connection_type = BaseHook.get_connection(self.conn_id).conn_type
        if connection_type not in self.supported_conn_type:
            raise ValueError(
                f"Connection type {connection_type} is not supported for {self.path}. "
                f"Supported types are {self.supported_conn_type}"
            )

    @property
    def smartopen_uri(self) -> str:
        """
        Changes the object URI (self.path) to a SmartOpen supported URI if necessary.
        By default, does not change the self.path.

        :return: URI compatible with SmartOpen for desired location.
        """
        return self.path

    @property
    def hook(self):
        raise NotImplementedError

    @property
    @abstractmethod
    def location_type(self) -> FileLocation:
        """Property to identify location type"""
        raise NotImplementedError

    @property
    @abstractmethod
    def paths(self) -> list[str]:
        """Resolve patterns in path"""
        raise NotImplementedError

    @property
    def transport_params(self) -> dict | None:  # skipcq: PYL-R0201
        """Get credentials required by smart open to access files"""
        return None

    @property
    @abstractmethod
    def size(self) -> int:
        """Return the size in bytes of the given file"""
        raise NotImplementedError

    @property
    @abstractmethod
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    @abstractmethod
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @staticmethod
    def is_valid_path(path: str) -> bool:
        """
        Check if the given path is either a valid URI or a local file

        :param path: Either local filesystem path or remote URI
        """
        try:
            BaseFileLocation.get_location_type(path)
        except ValueError:
            return False

        try:
            result = urlparse(path)

            if not (
                (result.scheme and result.netloc and (result.port or result.port is None))
                or os.path.isfile(path)
                or BaseFileLocation.check_non_existing_local_file_path(path)
                or glob.glob(result.path)
            ):
                return False

            return True
        except ValueError:
            return False

    @staticmethod
    def check_non_existing_local_file_path(path: str) -> bool:
        """Check if the path is valid by creating and temp file and then deleting it. Assumes the file don't exist"""
        try:
            Path(path).touch()
            os.remove(path)
        except OSError:
            return False
        return True

    @staticmethod
    def get_location_type(path: str) -> FileLocation:
        """Identify where a file is located

        :param path: Path to a file in the filesystem/Object stores
        """
        file_scheme = urlparse(path).scheme
        if file_scheme == "":
            location = FileLocation.LOCAL
        else:
            try:
                location = FileLocation(file_scheme)
            except ValueError:
                raise ValueError(f"Unsupported scheme '{file_scheme}' from path '{path}'")
        return location

    def exists(self) -> bool:
        """Check if the file exists or not"""
        try:
            with smart_open.open(self.smartopen_uri, mode="r", transport_params=self.transport_params):
                return True
        except OSError:
            return False

    @property
    def databricks_uri(self) -> str:
        """
        Return a Databricks compatible URI. In most scenarios, it will be self.path. An exception is Microsoft WASB.

        :return: self.path
        """
        return self.path

    def databricks_auth_settings(self) -> dict:
        """
        Required settings to upload this file into databricks. Only needed for cloud storage systems
        like S3
        :return: A dictionary of settings keys to settings values
        """
        return {}

    def __repr__(self) -> str:
        return f'{self.__class__.__name__}(path="{self.path}",conn_id="{self.conn_id}")'

    def __str__(self) -> str:
        """String representation of location"""
        return self.path

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        return self.path == other.path and self.conn_id == other.conn_id

    def __hash__(self) -> int:
        return hash((self.path, self.conn_id))

    def get_stream(self):
        """Create a file in the desired location using the smart_open.

        :param df: pandas dataframe
        """
        return smart_open.open(self.smartopen_uri, mode="wb", transport_params=self.transport_params)

    def get_snowflake_stage_auth_sub_statement(self) -> str:  # skipcq: PYL-R0201
        raise DatabaseCustomError("In order to create a stage, `storage_integration` is required.")

    @property
    def snowflake_stage_path(self) -> str:
        """
        Get the altered path if needed for stage creation in snowflake stage creation
        """
        return self.path
