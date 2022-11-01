from __future__ import annotations

import os
from urllib.parse import urlparse, urlunparse

from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class S3Location(BaseFileLocation):
    """Handler S3 object store operations"""

    location_type = FileLocation.S3

    @property
    def hook(self) -> S3Hook:
        return S3Hook(aws_conn_id=self.conn_id) if self.conn_id else S3Hook()

    @staticmethod
    def _parse_s3_env_var() -> tuple[str, str]:
        """Return S3 ID/KEY pair from environment vars"""
        return os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]

    @property
    def transport_params(self) -> dict:
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        """
        return {"client": self.hook.conn}

    @property
    def paths(self) -> list[str]:
        """Resolve S3 file paths with prefix"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        prefix = url.path[1:]
        prefixes = self.hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        paths = [urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes]
        return paths

    @property
    def size(self) -> int:
        """Return file size for S3 location"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        object_name = url.path
        if object_name.startswith("/"):
            object_name = object_name[1:]
        return self.hook.head_object(key=object_name, bucket_name=bucket_name).get("ContentLength") or -1
    
    def spark_config(self):
        configs = {'spark.hadoop.fs.s3a.access.key': self.hook.conn_config.aws_access_key_id,
                   'spark.hadoop.fs.s3a.secret.key': self.hook.conn_config.aws_secret_access_key,
                   'spark.hadoop.fs.s3a.impl':  'org.apache.hadoop.fs.s3a.S3AFileSystem'}
        if self.hook.conn_config.aws_session_token:
            configs[
                'spark.hadoop.fs.s3a.aws.credentials.provider'] = 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider'
            configs['spark.hadoop.fs.s3a.session.token'] = self.hook.conn_config.aws_session_token
        else:
            configs[
                'spark.hadoop.fs.s3a.aws.credentials.provider'] = 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider'

        configs["spark.sql.extensions"] = "io.delta.sql.DeltaSparkSessionExtension"
        configs['spark.jars.packages'] = 'org.apache.hadoop:hadoop-aws:3.2.0'
        return configs

    def spark_packages(self):
        return ["org.apache.hadoop:hadoop-aws:3.3.1"]

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        parsed_url = urlparse(self.path)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return urlparse(self.path).path
