from functools import cached_property

from airflow.providers.google.cloud.hooks.gcs import GCSHook
from gcsfs.core import GCSFileSystem

from universal_transfer_operator.data_providers.filesystem.base import BaseFileSystemProvider
from universal_transfer_operator.datasets.file import File


class GCSProvider(BaseFileSystemProvider):
    def __init__(self, dataset: File):
        self.dataset = dataset

    @cached_property
    def hook(self) -> GCSHook:
        if self.dataset.conn_id:
            return GCSHook(gcp_conn_id=self.dataset.conn_id)
        return GCSHook()

    @property
    def fsspec_path(self):
        fs = GCSFileSystem(
            token=self.hook.get_credentials(),
            project=self.hook.project_id,
        )
        return fs

    def read(self, ref):
        self.fsspec_path.get(rpath=self.dataset.path, lpath=ref)


import os

os.environ[
    "GOOGLE_APPLICATION_CREDENTIALS"
] = "/Users/kaxilnaik/Desktop/astronomer-dag-authoring-c52d8aaf2cc5.json"

a = GCSProvider(dataset=File("gs://dag-authoring/a.csv"))
a.read(ref="/Users/kaxilnaik/uto/")
