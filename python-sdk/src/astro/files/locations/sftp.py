from __future__ import annotations

import io
from contextlib import contextmanager
from urllib.parse import urlparse

from airflow.providers.sftp.hooks.sftp import SFTPHook

from astro.constants import FileLocation
from astro.exceptions import PermissionNotSetError
from astro.files.locations.base import BaseFileLocation


class SFTPLocation(BaseFileLocation):
    """
    Handler SFTP object store operations.
    While exporting to SFTP the user should have permission
    to create the file in the desired location.
    """

    location_type = FileLocation.SFTP
    supported_conn_type = {SFTPHook.conn_type}

    @property
    def hook(self) -> SFTPHook:
        return SFTPHook(ssh_conn_id=self.conn_id) if self.conn_id else SFTPHook()

    @property
    def transport_params(self) -> dict:
        """Get SFTP credentials in order to access remote file system"""
        client = self.hook.get_connection(self.conn_id)
        extra_options = client.extra_dejson
        if "key_file" in extra_options:
            key_file = extra_options.get("key_file")
            return {"connect_kwargs": {"key_filename": key_file}}
        elif client.password:
            return {"connect_kwargs": {"password": client.password}}
        raise PermissionNotSetError("SFTP credentials are not set in the connection.")

    @property
    def paths(self) -> list[str]:
        """Resolve SFTP file paths with prefix"""
        url = urlparse(self.path)
        uri = self.get_uri()
        full_paths = []
        prefixes = self.hook.get_tree_map(url.netloc, prefix=url.netloc + url.path)
        for keys in prefixes:
            if len(keys) > 0:
                full_paths.extend(keys)
        paths = [uri + "/" + path for path in full_paths]
        return paths

    @property
    def size(self) -> int:
        """Return file size for SFTP location"""
        url = urlparse(self.path)
        conn = self.hook.get_conn()
        stat = conn.stat(url.netloc + url.path).st_size
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
        client = self.hook.get_connection(self.conn_id)
        return client.get_uri()

    def get_stream(self):
        """return the custom SFTP read_buffer context to add file into given path"""
        return self.sftp_stream()

    @contextmanager
    def sftp_stream(self):
        """sftp_stream context to export a file to the given location."""
        buffer = io.BytesIO()
        try:
            yield buffer
        finally:
            buffer.seek(0)
            sftp = self.hook.get_conn()
            parsed_url = urlparse(self.path)
            sftp.putfo(buffer, parsed_url.netloc + parsed_url.path)
            buffer.close()
