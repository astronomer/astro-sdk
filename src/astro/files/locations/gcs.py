from typing import Dict, List, Optional
from urllib.parse import urlparse, urlunparse

from astro.files.locations.base import FileLocationAbstract
from astro.utils.dependencies import GCSClient, GCSHook, gcs


class GS(FileLocationAbstract):
    """Handler GS object store operations"""

    def get_transport_params(self, path: str, conn_id: Optional[str]) -> Dict:
        """get GCS credentials for storage
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        if conn_id:
            gcs_hook = GCSHook(conn_id)
            client = gcs_hook.get_conn()  # type: ignore
        else:
            client = GCSClient()

        return {"client": client}

    def get_paths(self, path: str, conn_id: Optional[str]) -> List[str]:
        """Resolve GS file paths with prefix
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        url = urlparse(path)
        bucket_name = url.netloc
        prefix = url.path[1:]
        hook = gcs.GCSHook(gcp_conn_id=conn_id) if conn_id else gcs.GCSHook()
        prefixes = hook.list(bucket_name=bucket_name, prefix=prefix)
        paths = [
            urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes
        ]
        return paths
