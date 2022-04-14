import glob
import os
from abc import ABC, abstractmethod
from typing import List, Optional
from urllib.parse import urlparse


class Location(ABC):
    """Base Location abstract class"""

    def __init__(self, path: str, conn_id: Optional[str] = None):
        """
        Location class constructor
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

    @abstractmethod
    def get_transport_params(self):
        """Get credentials required by smart open to access files"""
        raise NotImplementedError

    @staticmethod
    def is_valid_path(path: str) -> bool:
        """
        Check if the give path is either a valid URI or a local file
        :param path: Either local filesystem path or remote URI
        """
        result = urlparse(path)
        if not (
            all([result.scheme, result.netloc])
            or os.path.isfile(path)
            or glob.glob(result.path)
        ):
            return False
        return True
