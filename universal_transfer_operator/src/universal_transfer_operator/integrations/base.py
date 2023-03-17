from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any

import attr
from airflow.hooks.base import BaseHook

from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.utils import TransferParameters


@attr.define
class TransferIntegrationOptions(TransferParameters):
    """TransferIntegrationOptions for transfer integration configuration"""

    conn_id: str = attr.field(default="")


class TransferIntegration(ABC):
    """Basic implementation of a third party transfer."""

    def __init__(
        self,
        transfer_params: TransferIntegrationOptions = attr.field(
            factory=TransferIntegrationOptions,
            converter=lambda val: TransferIntegrationOptions(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.transfer_params = transfer_params
        # transfer mapping creates a mapping between various sources and destination, where
        # transfer is possible using the integration
        self.transfer_mapping: dict[str, str] = {}
        # TODO: add method for validation, transfer mapping, transfer params etc

    @property
    def hook(self) -> BaseHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    @abstractmethod
    def transfer_job(self, source_dataset: Dataset, destination_dataset: Dataset) -> Any:
        """
        Loads data from source dataset to the destination using ingestion config
        """
        raise NotImplementedError
