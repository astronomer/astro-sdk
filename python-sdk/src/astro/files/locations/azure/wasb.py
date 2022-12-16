from __future__ import annotations

from urllib.parse import urlparse, urlunparse

from airflow.providers.microsoft.azure.hooks.wasb import WasbHook

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class WASBLocation(BaseFileLocation):
    """Handler WASB object store operations"""

    location_type = FileLocation.WASB

    @property
    def hook(self) -> WasbHook:
        return WasbHook(wasb_conn_id=self.conn_id) if self.conn_id else WasbHook()

    @property
    def transport_params(self) -> dict:
        """get WASB credentials for storage"""
        client = self.hook.get_conn()
        return {"client": client}

    @property
    def paths(self) -> list[str]:
        """Resolve WASB file paths with prefix"""
        url = urlparse(self.path)
        container_name = url.netloc
        prefix = url.path[1:]
        prefixes = self.hook.get_blobs_list(container_name=container_name, prefix=prefix)
        paths = [urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes]
        return paths

    @property
    def size(self) -> int:
        """Return file size for WASB location"""
        url = urlparse(self.path)
        container_name = url.netloc
        object_name = url.path
        if object_name.startswith("/"):
            object_name = object_name[1:]
        return int(
            self.hook._get_blob_client(  # skipcq: PYL-W0212
                container_name=container_name, blob_name=object_name
            )
            .get_blob_properties()
            .size
        )

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