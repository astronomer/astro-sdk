from __future__ import annotations

from urllib.parse import urlparse

import smart_open
from airflow.providers.ftp.hooks.ftp import FTPHook

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class FTPLocation(BaseFileLocation):
    """Handler FTP object store operations"""

    location_type = FileLocation.FTP
    supported_conn_type = {FTPHook.conn_type}

    @property
    def hook(self) -> FTPHook:
        return FTPHook(ftp_conn_id=self.conn_id) if self.conn_id else FTPHook()

    @property
    def transport_params(self) -> dict:
        """get FTP credentials for remote file system"""
        return {}

    @property
    def paths(self) -> list[str]:
        """Resolve FTP file paths with prefix"""
        url = urlparse(self.path)
        conn = self.hook.get_connection(self.conn_id)
        uri = conn.get_uri()
        client = self.hook.get_conn()
        files = client.nlst("/" + url.netloc + url.path)
        # the above command checks if the path is a file or directory
        if len(files) > 0:
            paths = [uri + file for file in files]
        else:
            paths = [uri + "/" + url.netloc + url.path]
        return paths

    @property
    def size(self) -> int:
        """Return file size for FTP location"""
        url = urlparse(self.path)
        stat = self.hook.get_size(url.path)
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

    def get_uri(self):
        """Gets the URI of the connection"""
        conn = self.hook.get_connection(self.conn_id)
        return conn.get_uri()

    def get_stream(self):
        parsed_url = urlparse(self.path)
        path = f"{self.get_uri()}/{parsed_url.netloc}{parsed_url.path}"
        return smart_open.open(path, mode="wb", transport_params=self.transport_params)
