from functools import cached_property

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from universal_transfer_operator.data_providers.filesystem.base import BaseFileSystemProvider
from universal_transfer_operator.datasets.file import File

# TODO: fsspec vs PyFilesystem vs smart_open vs pyarrow.fs



class S3Provider(BaseFileSystemProvider):
    def __init__(self, dataset: File):
        self.dataset = dataset

    @cached_property
    def hook(self) -> S3Hook:
        if self.dataset.conn_id:
            return S3Hook(aws_conn_id=self.dataset.conn_id)
        return S3Hook()

    def read(self, ref):
        # self.read_using_s3fs(ref)
        # self.read_using_smart_open(ref)
        self.read_using_hook(ref)

    def read_using_smart_open(self, ref):
        import smart_open

        with smart_open.open(self.dataset.path, transport_params={"client": self.hook.conn}) as fout, open(
            ref, "w"
        ) as fin:
            content = fout.read()
            fin.write(content)

    def read_using_s3fs(self, ref):
        from s3fs.core import S3FileSystem

        credentials = self.hook.get_credentials()
        fs = S3FileSystem(
            key=credentials.access_key,
            secret=credentials.secret_key,
            token=credentials.token,
            config_kwargs=self.hook.config,
        )
        fs.get(rpath=self.dataset.path, lpath=ref)

    def read_using_hook(self, ref):
        self.hook.download_file(key=self.dataset.path, local_path=ref)


import os

# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/Users/kaxilnaik/Desktop/astronomer-dag-authoring-c52d8aaf2cc5.json"

a = S3Provider(dataset=File("s3://astro-sdk/imdb.csv"))
a.read(ref="/Users/kaxilnaik/uto/")
