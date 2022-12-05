from __future__ import annotations

from urllib.parse import urlparse

from airflow.providers.sftp.hooks.sftp import SFTPHook

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class SFTPLocation(BaseFileLocation):
    """Handler SFTP object store operations"""

    location_type = FileLocation.SFTP

    @property
    def hook(self) -> SFTPHook:
        return SFTPHook(ssh_conn_id=self.conn_id) if self.conn_id else SFTPHook()

    @property
    def transport_params(self) -> dict:
        """get SFTP credentials for remote file system"""
        client = self.hook.get_connection(self.conn_id)
        extra_options = client.extra_dejson
        if "key_file" in extra_options:
            self.key_file = extra_options.get("key_file")
            return {"connect_kwargs": {"key_filename": self.key_file}}
        return {}

    @property
    def paths(self) -> list[str]:
        """Resolve SFTP file paths with prefix"""
        url = urlparse(self.path)
        prefix = url.path[1:]
        print(prefix)
        url = urlparse(self.path)
        if self.hook.isdir(self.path):
            paths = self.hook.list_directory(self.path)
        else:
            paths = [self.path]
        print(paths)
        return paths

    @property
    def size(self) -> int:
        """Return file size for SFTP location"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        object_name = url.path
        if object_name.startswith("/"):
            object_name = object_name[1:]
        return int(self.hook.get_size(bucket_name=bucket_name, object_name=object_name))

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
