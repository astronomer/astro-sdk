from __future__ import annotations

import glob
import os
import pathlib
from os.path import exists
from urllib.parse import urlparse

import smart_open
from airflow.hooks.base import BaseHook

from universal_transfer_operator.data_providers.filesystem.base import BaseFilesystemProviders, FileStream


class LocalDataProvider(BaseFilesystemProviders):
    """Handler Local file path operations"""

    @property
    def paths(self) -> list[str]:
        """Resolve local filepath"""
        url = urlparse(self.dataset.path)
        path_object = pathlib.Path(url.path)
        if path_object.is_dir():
            paths = [str(filepath) for filepath in path_object.rglob("*")]
        else:
            paths = glob.glob(url.path)
        return paths

    def validate_conn(self):
        """Override as conn_id is not always required for local location."""

    @property
    def size(self) -> int:
        """Return the size in bytes of the given file.
        :return: File size in bytes
        """
        path = pathlib.Path(self.dataset.path)
        return os.path.getsize(path)

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return str(os.path.basename(self.dataset.path))

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return str(urlparse(self.dataset.path).path)

    def delete(self):
        """
        Delete a file/object if they exists
        """
        os.remove(self.dataset.path)

    def check_if_exists(self) -> bool:
        """Return true if the dataset exists"""
        return exists(self.dataset.path)

    def write_using_smart_open(self, source_ref: FileStream):
        """Write the source data from remote object i/o buffer to the dataset using smart open"""
        mode = "wb" if self.read_as_binary(source_ref.actual_filename) else "w"
        # destination_file = os.path.join(, os.path.basename(source_ref.actual_filename))
        with smart_open.open(self.dataset.path, mode=mode, transport_params=self.transport_params) as stream:
            stream.write(source_ref.remote_obj_buffer.read())
        return self.dataset.path

    @property
    def hook(self) -> BaseHook:
        """Return an instance of the Airflow hook."""
        raise NotImplementedError
