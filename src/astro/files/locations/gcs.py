from typing import Dict, List
from urllib.parse import urlparse, urlunparse

from astro.constants import FileLocation
from astro.files.locations.base import Location
from astro.utils.dependencies import GCSClient, GCSHook, gcs


class GCS(Location):
    """Handler GS object store operations"""

    location_type = FileLocation.GS

    def get_transport_params(self) -> Dict:
        """get GCS credentials for storage"""
        if self.conn_id:
            gcs_hook = GCSHook(self.conn_id)
            client = gcs_hook.get_conn()  # type: ignore
        else:
            client = GCSClient()

        return {"client": client}

    def get_paths(self) -> List[str]:
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
