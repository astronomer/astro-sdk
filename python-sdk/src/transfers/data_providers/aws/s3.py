from __future__ import annotations

import logging
import os

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from transfers.constants import FileLocation, TransferMode
from transfers.data_providers import create_dataprovider
from transfers.data_providers.base import DataProviders
from transfers.datasets.base import UniversalDataset as Dataset
from transfers.utils import get_dataset_connection_type

from astro.constants import LoadExistStrategy


class S3DataProvider(DataProviders):
    """
    DataProviders interactions with S3 Dataset.
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
            FileLocation.GS,
            FileLocation.S3,
            FileLocation.AWS,
            FileLocation.google_cloud_platform,
        }

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        return S3Hook(
            aws_conn_id=self.conn_id,
            verify=self.extra.get("verify", None),
            transfer_config_args=self.extra.get("transfer_config_args", None),
            extra_args=self.extra.get("s3_extra_args", {}),
        )

    def check_if_exists(self, dataset: Dataset) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

    def check_if_transfer_supported(self, source_dataset: Dataset) -> bool:
        """
        Checks if the transfer is supported from source to destination based on source_dataset.
        """
        source_connection_type = get_dataset_connection_type(source_dataset)
        return FileLocation(source_connection_type) in self.transfer_mapping

    def load_data_from_source(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        """
        Loads data from source dataset to the destination using data provider
        """
        if not self.check_if_transfer_supported(source_dataset=source_dataset):
            raise ValueError("Transfer not supported yet.")
        source_connection_type = get_dataset_connection_type(source_dataset)
        if source_connection_type == "google_cloud_platform":
            return self.load_data_from_gcs(source_dataset, destination_dataset)

    def load_data_from_gcs(self, source_dataset: Dataset, destination_dataset: Dataset) -> None:
        source_dataprovider = create_dataprovider(
            dataset=source_dataset,
            transfer_params=self.transfer_params,
            transfer_mode=self.transfer_mode,
            if_exists=self.if_exists,
        )
        source_hook = source_dataprovider.hook
        logging.info(
            "Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s",
            source_dataprovider.get_bucket_name(source_dataset),  # type: ignore
            source_dataset.extra.get("delimiter", None),
            source_dataset.extra.get("prefix", None),
        )
        files = source_hook.list(
            bucket_name=source_dataprovider.get_bucket_name(source_dataset),  # type: ignore
            prefix=source_dataset.extra.get("prefix", None),
            delimiter=source_dataset.extra.get("delimiter", None),
        )
        dest_s3_key = destination_dataset.path
        if not destination_dataset.extra.get(
            "keep_directory_structure", False
        ) and destination_dataset.extra.get("prefix", None):
            dest_s3_key = os.path.join(
                self.get_s3_key(destination_dataset), destination_dataset.extra.get("prefix", "")
            )

        if self.if_exists != "replace":
            bucket_name, prefix = self.hook.parse_s3_url(dest_s3_key)
            # look for the bucket and the prefix to avoid look into
            # parent directories/keys
            existing_files = self.hook.list_keys(bucket_name, prefix=prefix)
            # in case that no files exists, return an empty array to avoid errors
            existing_files = existing_files if existing_files is not None else []
            # remove the prefix for the existing files to allow the match
            existing_files = [file.replace(prefix, "", 1) for file in existing_files]
            files = list(set(files) - set(existing_files))

        if files:
            for file in files:
                with source_hook.provide_file(
                    object_name=file, bucket_name=source_dataprovider.get_bucket_name(source_dataset)  # type: ignore
                ) as local_tmp_file:
                    dest_key = os.path.join(dest_s3_key, file)
                    logging.info("Saving file to %s", dest_key)
                    self.hook.load_file(
                        filename=local_tmp_file.name,
                        key=dest_key,
                        replace="replace",
                        acl_policy=destination_dataset.extra.get("s3_acl_policy", None),
                    )

            logging.info("All done, uploaded %d files to S3", len(files))
        else:
            logging.info("In sync, no files needed to be uploaded to S3")

        return files

    def get_bucket_name(self, dataset: Dataset) -> str:
        bucket_name, key = self.hook.parse_s3_url(dataset.path)
        return bucket_name

    def get_s3_key(self, dataset: Dataset) -> str:
        bucket_name, key = self.hook.parse_s3_url(dataset.path)
        return key

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
