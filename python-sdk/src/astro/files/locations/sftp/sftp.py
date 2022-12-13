from __future__ import annotations

import os
from urllib.parse import urlparse, urlunparse

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
            key_file = extra_options.get("key_file")
            return {"connect_kwargs": {"key_filename": key_file}}
        elif client.password:
            return {"connect_kwargs": {"password": client.password}}
        return {}

    @property
    def paths(self) -> list[str]:
        """Resolve SFTP file paths with prefix"""
        url = urlparse(self.path)
        client = self.hook.get_connection(self.conn_id)
        port = str(client.port or 22)
        if self.hook.isdir(url.path):
            prefixes = self.hook.list_directory(url.path)
            paths = [
                urlunparse((url.scheme, url.netloc + ":" + port, os.path.join(url.path, keys), "", "", ""))
                for keys in prefixes
            ]
        else:
            paths = [urlunparse((url.scheme, url.netloc + ":" + port, url.path, "", "", ""))]
        return paths

    @property
    def size(self) -> int:
        """Return file size for SFTP location"""
        url = urlparse(self.path)
        conn = self.hook.get_conn()
        stat = conn.stat(url.path).st_size
        return int(stat) or -1

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
