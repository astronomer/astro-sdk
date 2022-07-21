from __future__ import annotations

from abc import ABC, abstractmethod

from src.universal_transfer_operator.data_providers.base import DataProviders
from src.universal_transfer_operator.dataset import Dataset


class DataTransfer(ABC):
    """
    Abstract base class for DataTransfer, data transfers are meant as an interface to
    transfer dataset between external systems using relevant DataProviders.
    FileSystemToDatabaseTransfer, FileSystemToFileSystemTransfer, DatabaseToFileSystemTransfer,
    DatabaseToDatabaseTransfer etc return object that can transfer dataset between source
    and destination using respective data providers.
    """

    @abstractmethod
    def load_dataset_from_source(
        self, source_dataset: Dataset, source_data_provider: DataProviders
    ) -> object:
        """
        Read the dataset from the specified URI and conn_id.
        """
        pass

    @abstractmethod
    def transfer_dataset(
        self,
        source_dataset: Dataset,
        destination_dataset: Dataset,
        source_data_provider: DataProviders,
        destination_data_provider: DataProviders,
    ) -> None:
        """
        Transfers the data between source and destination using Data Providers.
        """
        pass
