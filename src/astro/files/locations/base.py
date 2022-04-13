from abc import ABC, abstractmethod
from typing import List, Optional


class LocationAbstract(ABC):
    """Base Location abstract class"""

    @abstractmethod
    def get_paths(self, path: str, conn_id: Optional[str]) -> List[str]:
        """Resolve patterns in path
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        raise NotImplementedError

    @abstractmethod
    def get_transport_params(self, path: str, conn_id: Optional[str]):
        """Get credentials required by smart open to access files
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        raise NotImplementedError
