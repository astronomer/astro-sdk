from __future__ import annotations

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from transfers.constants import FileLocation, TransferMode
from transfers.data_providers.base import DataProviders
from transfers.datasets.base import UniversalDataset as Dataset

from astro.constants import LoadExistStrategy


class GCSDataProvider(DataProviders):
    """
    DataProviders interactions with GS Dataset.
    """

    def __init__(
        self,
        conn_id: str,
        extra: dict = {},
        transfer_params: dict = {},
        transfer_mode: TransferMode = TransferMode.NONNATIVE,
        if_exists: LoadExistStrategy = "replace",
    ):
        super().__init__(
            conn_id=conn_id,
            extra=extra,
            transfer_params=transfer_params,
            transfer_mode=transfer_mode,
            if_exists=if_exists,
        )
        self.transfer_mapping: set = {
            FileLocation.AWS,
            FileLocation.google_cloud_platform,
        }
        self.LOAD_DATA_FROM_SOURCE = {
            "google_cloud_platform": "load_data_from_gcs",
            "aws": "load_data_from_s3",
        }

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        return GCSHook(
            gcp_conn_id=self.conn_id,
            delegate_to=self.extra.get("delegate_to", None),
            impersonation_chain=self.extra.get("google_impersonation_chain", None),
        )

    def check_if_exists(self, dataset: Dataset) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def get_bucket_name(self, source_dataset: Dataset) -> str:
        bucket_name, blob = _parse_gcs_url(gsurl=source_dataset.path)
        return bucket_name

    def load_data_from_gcs(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        raise NotImplementedError

    def load_data_from_s3(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
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
