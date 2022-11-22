from __future__ import annotations

from abc import abstractmethod

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from uto.data_providers.base import DataProviders
from uto.datasets.base import UniversalDataset as Dataset
from uto.utils import FileLocation, get_dataset_connection_type

from astro.constants import LoadExistStrategy


class GSDataProviders(DataProviders):
    """
    DataProviders interactions with GS Dataset.
    """

    def __init__(
        self,
        conn_id: str,
        optimization_params: dict | None,
        extras: dict = {},
        use_optimized_transfer: bool = True,
        if_exists: LoadExistStrategy = "replace",
    ):
        super().__init__(
            conn_id=conn_id,
            extras=extras,
            optimization_params=optimization_params,
            use_optimized_transfer=use_optimized_transfer,
            if_exists=if_exists,
        )
        self.transfer_mapping: set = {FileLocation.GS, FileLocation.S3}

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        return GCSHook(
            gcp_conn_id=self.conn_id,
            delegate_to=self.extras.get("delegate_to"),
            impersonation_chain=self.extras.get("google_impersonation_chain"),
        )

    def check_if_exists(self, dataset: Dataset) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def get_bucket_name(self, source_dataset: Dataset) -> str:
        bucket_name, blob = _parse_gcs_url(gsurl=source_dataset.path)
        return bucket_name

    @abstractmethod
    def load_data_from_source(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        """
        Loads data from source dataset to the destination using data provider
        """
        if not self.check_if_transfer_supported(source_dataset=source_dataset):
            raise ValueError("Transfer not supported yet.")
        source_connection_type = get_dataset_connection_type(source_dataset)
        if source_connection_type == "gs":
            return self.load_data_from_gcs(source_dataset, destination_dataset)

    def load_data_from_gcs(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
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
