from __future__ import annotations

from functools import cached_property
from urllib.parse import ParseResult, urlparse, urlunparse

import attr
import smart_open
from airflow.providers.sftp.hooks.sftp import SFTPHook

from universal_transfer_operator.constants import Location, TransferMode
from universal_transfer_operator.data_providers.filesystem.base import BaseFilesystemProviders, FileStream
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.integrations.base import TransferIntegrationOptions


class SFTPDataProvider(BaseFilesystemProviders):
    """
    DataProviders interactions with GS Dataset.
    """

    def __init__(
        self,
        dataset: File,
        transfer_params: TransferIntegrationOptions = attr.field(
            factory=TransferIntegrationOptions,
            converter=lambda val: TransferIntegrationOptions(**val) if isinstance(val, dict) else val,
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
    def hook(self) -> SFTPHook:
        """Return an instance of the SFTPHook Airflow hook."""
        return SFTPHook(ssh_conn_id=self.dataset.conn_id)

    def delete(self):
        """
        Delete a file/object if they exists
        """
        self.hook.delete_file(path=self.dataset.path.replace("sftp://", "/"))

    def check_if_exists(self):
        """Return true if the dataset exists"""
        return self.hook.path_exists(self.dataset.path.replace("sftp://", "/"))

    @property
    def paths(self) -> list[str]:
        """
        Resolve SFTP file paths with netloc of self.dataset.path as prefix. Paths are added if they start with prefix

        Example - if there are multiple paths like
            - sftp://upload/test.csv
            - sftp://upload/test.json
            - sftp://upload/home.parquet
            - sftp://upload/sample.ndjson

        If self.dataset.path is "sftp://upload/test" will return sftp://upload/test.csv and sftp://upload/test.json
        """
        url = urlparse(self.dataset.path)
        uri = self.get_uri()
        full_paths = []
        prefixes = self.hook.get_tree_map(url.netloc, prefix=url.netloc + url.path)
        for keys in prefixes:
            if len(keys) > 0:
                full_paths.extend(keys)
        # paths = ["/" + path for path in full_paths]
        paths = [uri + "/" + path for path in full_paths]
        return paths

    @property
    def transport_params(self) -> dict:
        """get SFTP credentials for storage"""
        client = self.hook.get_connection(self.dataset.conn_id)
        extra_options = client.extra_dejson
        if "key_file" in extra_options:
            key_file = extra_options.get("key_file")
            return {"connect_kwargs": {"key_filename": key_file}}
        elif client.password:
            return {"connect_kwargs": {"password": client.password}}
        raise ValueError("SFTP credentials are not set in the connection.")

    def get_uri(self):
        client = self.hook.get_connection(self.dataset.conn_id)
        return client.get_uri()

    @staticmethod
    def _get_url_path(dst_url: ParseResult, src_url: ParseResult) -> str:
        """
        Get correct file path, priority is given to destination file path.
        :return: URL path
        """
        path = dst_url.path if dst_url.__getattribute__("path") else src_url.path
        # Casting AnyStr to str
        return str(dst_url.hostname) + path

    def get_complete_url(self, dst_url: str, src_url: str) -> str:
        """
        Get complete url with host, port, username, password if they are not provided in the `dst_url`
        """
        complete_url = urlparse(self.get_uri())
        _dst_url = urlparse(dst_url)
        _src_url = urlparse(src_url)

        path = self._get_url_path(dst_url=_dst_url, src_url=_src_url)

        final_url = complete_url._replace(path=path)

        return urlunparse(final_url)

    def write_using_smart_open(self, source_ref: FileStream) -> str:
        """Write the source data from remote object i/o buffer to the dataset using smart open
        :param source_ref: FileStream object of source dataset
        :return: File path that is the used for write pattern
        """
        mode = "wb" if self.read_as_binary(source_ref.actual_file.path) else "w"
        complete_url = self.get_complete_url(self.dataset.path, source_ref.actual_file.path)
        with smart_open.open(complete_url, mode=mode, transport_params=self.transport_params) as stream:
            stream.write(source_ref.remote_obj_buffer.read())
        return complete_url

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

    @property
    def size(self) -> int:
        """Return file size for SFTP location"""
        url = urlparse(self.dataset.path)
        conn = self.hook.get_conn()
        stat = conn.stat(url.netloc + url.path).st_size
        return int(stat) if stat else -1
