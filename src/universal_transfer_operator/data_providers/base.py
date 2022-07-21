from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

from airflow.hooks.base import BaseHook


class DataProviders(ABC, BaseHook):
    """
    Abstract base class for DataProviders, data providers are meant as an interface to
    interact with external systems. S3DataProvider, GCPDataProvider, HDFSDataProvider,
    MySQLDataProvider etc return object that can handle the connection and interaction to
    specific instances of these systems, read dataset, write dataset and expose consistent
    methods to interact with them.
    """

    @abstractmethod
    def read_dataset(self, conn_id: str, uri: str) -> object:
        """
        Read the dataset from the specified URI and conn_id.
        """
        pass

    @abstractmethod
    def write_dataset(self, conn_id: str, uri: str, data: Any) -> None:
        """
        Writes the data to the given URI using conn_id.
        """
        pass
