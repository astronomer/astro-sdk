from __future__ import annotations

import glob
import os
import pathlib
from urllib.parse import urlparse

from universal_transfer_operator.data_providers.filesystem.base import BaseFilesystemProviders


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
        basename: str = os.path.basename(self.dataset.path)
        return basename

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        path: str = urlparse(self.dataset.path).path
        return path
