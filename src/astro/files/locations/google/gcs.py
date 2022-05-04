from typing import Dict, List
from urllib.parse import urlparse, urlunparse

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation
from astro.utils.dependencies import gcs


class GCSLocation(BaseFileLocation):
    """Handler GS object store operations"""

    location_type = FileLocation.GS

    @property
    def transport_params(self) -> Dict:
        """get GCS credentials for storage"""
        hook = gcs.GCSHook(gcp_conn_id=self.conn_id) if self.conn_id else gcs.GCSHook()
        client = hook.get_conn()
        return {"client": client}

    @property
    def paths(self) -> List[str]:
        """Resolve GS file paths with prefix"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        prefix = url.path[1:]
        hook = gcs.GCSHook(gcp_conn_id=self.conn_id) if self.conn_id else gcs.GCSHook()
        prefixes = hook.list(bucket_name=bucket_name, prefix=prefix)
        paths = [
            urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes
        ]
        return paths

    @property
    def size(self) -> int:
        return -1
