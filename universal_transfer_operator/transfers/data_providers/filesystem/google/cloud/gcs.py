from __future__ import annotations

import logging
import os
from tempfile import NamedTemporaryFile

from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url
from constants import FileLocation, LoadExistStrategy, TransferMode
from data_providers.filesystem.base import BaseFilesystemProviders, TempFile
from datasets.base import UniversalDataset as Dataset


class GCSDataProvider(BaseFilesystemProviders):
    """
    DataProviders interactions with GS Dataset.
    """

    def __init__(
        self,
        dataset: Dataset,
        transfer_params: dict = {},
        transfer_mode: TransferMode = TransferMode.NONNATIVE,
        if_exists: LoadExistStrategy = "replace",
    ):
        super().__init__(
            dataset=dataset,
            transfer_params=transfer_params,
            transfer_mode=transfer_mode,
            if_exists=if_exists,
        )
        self.transfer_mapping: set = {
            FileLocation.AWS,
            FileLocation.google_cloud_platform,
        }

    @property
    def hook(self) -> GCSHook:
        """Return an instance of the database-specific Airflow hook."""
        return GCSHook(
            gcp_conn_id=self.dataset.conn_id,
            delegate_to=self.dataset.extra.get("delegate_to", None),
            impersonation_chain=self.dataset.extra.get("google_impersonation_chain", None),
        )

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        return self.hook.exists(bucket_name=self.get_bucket_name(), object_name=self.get_blob_name())

    def read(self) -> list[TempFile]:
        """Read the file from dataset and write to local file location"""
        if not self.check_if_exists():
            raise ValueError(f"{self.dataset.path} doesn't exits")
        logging.info(
            "Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s",
            self.get_bucket_name(),  # type: ignore
            self.dataset.extra.get("delimiter", None),
            self.dataset.extra.get("prefix", None),
        )
        files = self.hook.list(
            bucket_name=self.get_bucket_name(),  # type: ignore
            prefix=self.dataset.extra.get("prefix", None),
            delimiter=self.dataset.extra.get("delimiter", None),
        )
        local_filename = []
        if files:
            for file in files:
                _, _, file_name = file.rpartition("/")
                with NamedTemporaryFile(suffix=file_name, delete=False) as tmp_file:
                    self.hook.download(
                        bucket_name=self.get_bucket_name(),
                        object_name=self.get_blob_name(),
                        filename=tmp_file.name,
                    )
                    local_filename.append(TempFile(tmp_file=tmp_file, actual_filename=file_name))
        return local_filename

    def write(self, source_ref: list[TempFile]) -> list[str]:
        """Write the file from local file location to the dataset"""
        bucket_name = self.get_bucket_name()
        object_prefix = self.get_blob_name()

        if source_ref:
            for file in source_ref:
                # There will always be a '/' before file because it is
                # enforced at instantiation time
                dest_gcs_object = object_prefix + file
                self.hook.upload(
                    bucket_name=bucket_name,
                    object_name=dest_gcs_object,
                    filename=file.name,
                    gzip=self.dataset.extra.get("gzip", False),
                )
            logging.info("All done, uploaded %d files to Google Cloud Storage", len(source_ref))
        else:
            logging.info("In sync, no files needed to be uploaded to Google Cloud Storage")
        return source_ref

    def __enter__(self):
        return self

    def __exit__(self, file_list: list[TempFile]):
        for file in file_list:
            if os.path.exists(file.tmp_file.name):
                os.remove(file.tmp_file.name)

    def get_bucket_name(self) -> str:
        bucket_name, _ = _parse_gcs_url(gsurl=self.dataset.path)
        return bucket_name

    def get_blob_name(self) -> str:
        bucket_name, blob = _parse_gcs_url(gsurl=self.dataset.path)
        return blob

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
