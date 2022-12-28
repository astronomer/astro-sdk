from __future__ import annotations

import os

from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from constants import FileLocation, LoadExistStrategy, TransferMode
from data_providers.filesystem.base import BaseFilesystemProviders, TempFile
from datasets.base import UniversalDataset as Dataset


class S3DataProvider(BaseFilesystemProviders):
    """
    DataProviders interactions with S3 Dataset.
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
    def hook(self) -> S3Hook:
        """Return an instance of the database-specific Airflow hook."""
        return S3Hook(
            aws_conn_id=self.dataset.conn_id,
            verify=self.dataset.extra.get("verify", None),
            transfer_config_args=self.dataset.extra.get("transfer_config_args", None),
            extra_args=self.dataset.extra.get("s3_extra_args", {}),
        )

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        return self.hook.check_for_key(key=self.dataset.path)

    def read(self) -> list[TempFile]:
        """Read the file from dataset and write to local file location"""
        if not self.check_if_exists():
            raise ValueError(f"{self.dataset.path} doesn't exits")
        files = self.hook.list_keys(
            bucket_name=self.get_bucket_name(),
            prefix=self.get_s3_key(),
            delimiter=self.dataset.extra.get("delimiter"),
        )
        local_file_paths = []
        for file in files:
            local_path = self.hook.download_file(key=file)
            local_file_paths.append(local_path)
        return local_file_paths

    def write(self, source_ref: list[TempFile]) -> list[TempFile]:
        """Write the file from local file location to the dataset"""

        dest_s3_key = self.dataset.path

        if not self.dataset.extra.get("keep_directory_structure", False) and self.dataset.extra.get(
            "prefix", None
        ):
            dest_s3_key = os.path.join(dest_s3_key, self.dataset.extra.get("prefix", ""))

        if self.if_exists != "replace":
            bucket_name, prefix = self.hook.parse_s3_url(dest_s3_key)
            # look for the bucket and the prefix to avoid look into
            # parent directories/keys
            existing_files = self.hook.list_keys(bucket_name, prefix=prefix)
            # in case that no files exists, return an empty array to avoid errors
            existing_files = existing_files if existing_files is not None else []
            # remove the prefix for the existing files to allow the match
            existing_files = [file.replace(prefix, "", 1) for file in existing_files]
            source_ref = list(set(source_ref) - set(existing_files))

        if source_ref:
            for file in source_ref:
                if os.path.exists(file.tmp_file.name):
                    dest_key = os.path.join(dest_s3_key, os.path.basename(file.actual_filename))
                    self.hook.load_file(
                        filename=file.tmp_file.name,
                        key=dest_key,
                        replace="replace",
                        acl_policy=self.dataset.extra.get("s3_acl_policy", None),
                    )

        return source_ref

    def __enter__(self):
        return self

    def __exit__(self, file_list: list[TempFile]):
        for file in file_list:
            if os.path.exists(file.tmp_file.name):
                os.remove(file.tmp_file.name)

    def get_bucket_name(self) -> str:
        bucket_name, key = self.hook.parse_s3_url(self.dataset.path)
        return bucket_name

    def get_s3_key(self) -> str:
        bucket_name, key = self.hook.parse_s3_url(self.dataset.path)
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
