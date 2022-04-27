import glob
import os
from abc import ABC, abstractmethod
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from astro.constants import FileLocation


class Location(ABC):
    """Base Location abstract class"""

    def __init__(self, path: str, conn_id: Optional[str] = None):
        """
        Manages and provide interface for the operation for all the supported locations.

        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        if Location.is_valid_path(path):
            self.path = path
            self.conn_id = conn_id
        else:
            raise ValueError(f"Invalid path: '{path}'")

    @property
    @abstractmethod
    def location_type(self):
        """Property to identify location type"""
        raise NotImplementedError

    @abstractmethod
    def get_paths(self) -> List[str]:
        """Resolve patterns in path"""
        raise NotImplementedError

    def get_transport_params(self) -> Union[Dict, None]:
        """Get credentials required by smart open to access files"""
        return None

    @staticmethod
    def is_valid_path(path: str) -> bool:
        """
        Check if the given path is either a valid URI or a local file

        :param path: Either local filesystem path or remote URI
        """
        try:
            Location.get_location_type(path)
        except ValueError:
            return False

        result = urlparse(path)
        if not (
            (result.scheme and result.netloc)
            or os.path.isfile(path)
            or glob.glob(result.path)
        ):
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
