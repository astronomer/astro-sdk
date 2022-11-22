from __future__ import annotations

import logging
import os
from abc import abstractmethod

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from uto.data_providers.base import DataProviders
from uto.datasets.base import UniversalDataset as Dataset
from uto.utils import FileLocation, create_dataprovider, get_dataset_connection_type

from astro.constants import LoadExistStrategy


class S3DataProviders(DataProviders):
    """
    DataProviders interactions with S3 Dataset.
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
        return S3Hook(
            aws_conn_id=self.conn_id,
            verify=self.extras.get("verify", None),
            transfer_config_args=self.extras.get("transfer_config_args", None),
            extra_args=self.extras.get("s3_extra_args", {}),
        )

    def check_if_exists(self, dataset: Dataset) -> bool:
        """Return true if the dataset exists"""
        raise NotImplementedError

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
        source_dataprovider = create_dataprovider(
            dataset=source_dataset,
            extras=self.extras,
            optimization_params=self.optimization_params,
            use_optimized_transfer=self.use_optimized_transfer,
        )
        source_hook = source_dataprovider.hook()
        logging.info(
            "Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s",
            source_dataprovider.get_bucket_name(source_dataset),  # type: ignore
            self.extras.get("delimiter", None),
            self.extras.get("prefix", None),
        )
        files = source_hook.list(
            bucket_name=source_dataprovider.get_bucket_name(source_dataset),  # type: ignore
            prefix=self.extras.get("prefix", None),
            delimiter=self.extras.get("delimiter", None),
        )

        if not self.extras.get("keep_directory_structure", False) and self.extras.get("prefix", None):
            dest_s3_key = os.path.join(self.get_s3_key(destination_dataset), self.extras.get("prefix", ""))

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
                        replace=self.if_exists,
                        acl_policy=self.extras.get("s3_acl_policy"),
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
