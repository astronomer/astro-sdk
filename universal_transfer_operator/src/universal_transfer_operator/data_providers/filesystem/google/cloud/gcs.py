from __future__ import annotations

import logging
from functools import cached_property
from tempfile import NamedTemporaryFile
from typing import Sequence

import attr
from airflow.providers.google.cloud.hooks.gcs import GCSHook, _parse_gcs_url

from universal_transfer_operator.constants import Location, TransferMode
from universal_transfer_operator.data_providers.filesystem.base import (
    BaseFilesystemProviders,
    Path,
    TempFile,
    contextmanager,
)
from universal_transfer_operator.datasets.base import UniversalDataset as Dataset
from universal_transfer_operator.universal_transfer_operator import TransferParameters


class GCSDataProvider(BaseFilesystemProviders):
    """
    DataProviders interactions with GS Dataset.
    """

    def __init__(
        self,
        dataset: Dataset,
        transfer_params: TransferParameters = attr.field(
            factory=TransferParameters,
            converter=lambda val: TransferParameters(**val) if isinstance(val, dict) else val,
        ),
        transfer_mode: TransferMode = TransferMode.NONNATIVE,
    ):
        super().__init__(
            dataset=dataset,
            transfer_params=transfer_params,
            transfer_mode=transfer_mode,
        )
        self.transfer_mapping = {
            Location.S3,
            Location.GS,
        }

    @cached_property
    def hook(self) -> GCSHook:
        """Return an instance of the database-specific Airflow hook."""
        return GCSHook(
            gcp_conn_id=self.dataset.conn_id,
            delegate_to=self.delegate_to,
            impersonation_chain=self.google_impersonation_chain,
        )

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        return self.hook.exists(bucket_name=self.bucket_name, object_name=self.blob_name)

    @contextmanager
    def read(self) -> list[TempFile]:
        """Read the file from dataset and write to local file location"""
        if not self.check_if_exists():
            raise ValueError(f"{self.dataset.path} doesn't exits")

        logging.info(
            "Getting list of the files. Bucket: %s; Delimiter: %s; Prefix: %s",
            self.bucket_name,  # type: ignore
            self.delimiter,
            self.prefix,
        )
        files = self.hook.list(
            bucket_name=self.bucket_name,  # type: ignore
            prefix=self.prefix,
            delimiter=self.delimiter,
        )

        try:
            local_file_paths = []
            if files:
                for file in files:
                    local_file_paths.append(self.download_file(file))
            yield local_file_paths
        finally:
            # Clean up the local files
            self.cleanup(local_file_paths)

    def write(self, source_ref: list[TempFile]) -> list[str]:
        """Write the file from local file location to the dataset"""
        destination_objects = []
        if source_ref:
            for file in source_ref:
                destination_objects.append(self.upload_file(file))
            logging.info("All done, uploaded %d files to Google Cloud Storage", len(source_ref))
        else:
            logging.info("In sync, no files needed to be uploaded to Google Cloud Storage")
        return destination_objects

    def upload_file(self, file: TempFile):
        """Upload file to GCS and return path"""
        # There will always be a '/' before file because it is
        # enforced at instantiation time
        dest_gcs_object = self.blob_name + file.actual_filename.name
        self.hook.upload(
            bucket_name=self.bucket_name,
            object_name=dest_gcs_object,
            filename=file.tmp_file.as_posix(),
            gzip=self.gzip,
        )
        return dest_gcs_object

    def download_file(self, file) -> TempFile:
        """Download file and save to temporary path."""
        _, _, file_name = file.rpartition("/")
        with NamedTemporaryFile(suffix=file_name, delete=False) as tmp_file:
            self.hook.download(
                bucket_name=self.bucket_name,
                object_name=file,
                filename=tmp_file.name,
            )
            return TempFile(tmp_file=Path(tmp_file.name), actual_filename=Path(file_name))

    @property
    def delegate_to(self) -> str | None:
        return self.dataset.extra.get("delegate_to", None)

    @property
    def google_impersonation_chain(self) -> str | Sequence[str] | None:
        return self.dataset.extra.get("google_impersonation_chain", None)

    @property
    def delimiter(self) -> str | None:
        return self.dataset.extra.get("delimiter", None)

    @property
    def bucket_name(self) -> str:
        bucket_name, _ = _parse_gcs_url(gsurl=self.dataset.path)
        return bucket_name

    @property
    def prefix(self) -> str | None:
        return self.dataset.extra.get("prefix", None)

    @property
    def gzip(self) -> bool | None:
        return self.dataset.extra.get("gzip", False)

    @property
    def blob_name(self) -> str:
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
