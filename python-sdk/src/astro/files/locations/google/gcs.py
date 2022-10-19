from __future__ import annotations

from urllib.parse import urlparse, urlunparse

from airflow.providers.google.cloud.hooks.gcs import GCSHook

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class GCSLocation(BaseFileLocation):
    """Handler GS object store operations"""

    location_type = FileLocation.GS

    @property
    def hook(self) -> GCSHook:
        return GCSHook(gcp_conn_id=self.conn_id) if self.conn_id else GCSHook()

    @property
    def transport_params(self) -> dict:
        """get GCS credentials for storage"""
        client = self.hook.get_conn()
        return {"client": client}

    @property
    def paths(self) -> list[str]:
        """Resolve GS file paths with prefix"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        prefix = url.path[1:]
        prefixes = self.hook.list(bucket_name=bucket_name, prefix=prefix)
        paths = [urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes]
        return paths

    @property
    def size(self) -> int:
        """Return file size for GCS location"""
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
