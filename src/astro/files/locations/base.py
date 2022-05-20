import glob
import os
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

import smart_open

from astro.constants import FileLocation


class BaseFileLocation(ABC):
    """Base Location abstract class"""

    template_fields = ("path", "conn_id")

    def __init__(self, path: str, conn_id: Optional[str] = None):
        """
        Manages and provide interface for the operation for all the supported locations.

        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        self.path = path
        self.conn_id = conn_id

    @property
    @abstractmethod
    def location_type(self):
        """Property to identify location type"""
        raise NotImplementedError

    @property
    @abstractmethod
    def paths(self) -> List[str]:
        """Resolve patterns in path"""
        raise NotImplementedError

    @property
    def transport_params(self) -> Union[Dict, None]:  # skipcq: PYL-R0201
        """Get credentials required by smart open to access files"""
        return None

    @property
    @abstractmethod
    def size(self):
        """Return the size in bytes of the given file"""
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
                (
                    result.scheme
                    and result.netloc
                    and (result.port or result.port is None)
                )
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
                raise ValueError(
                    f"Unsupported scheme '{file_scheme}' from path '{path}'"
                )
        return location

    def exists(self) -> bool:
        """Check if the file exists or not"""
        try:
            with smart_open.open(
                self.path, mode="r", transport_params=self.transport_params
            ):
                return True
        except OSError:
            return False

    def __repr__(self):
        return f'{self.__class__.__name__}(path="{self.path}",conn_id="{self.conn_id}")'

    def __str__(self):
        """String representation of location"""
        return self.path
