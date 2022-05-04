import os
from typing import Dict, List, Tuple
from urllib.parse import urlparse, urlunparse

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation
from astro.utils.dependencies import s3


class S3Location(BaseFileLocation):
    """Handler S3 object store operations"""

    location_type = FileLocation.S3

    @staticmethod
    def _parse_s3_env_var() -> Tuple[str, str]:
        """Return S3 ID/KEY pair from environment vars"""
        return os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]

    @property
    def transport_params(self) -> Dict:
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        """
        hook = s3.S3Hook(aws_conn_id=self.conn_id) if self.conn_id else s3.S3Hook()
        session = hook.get_session()
        return {"client": session.client("s3")}

    @property
    def paths(self) -> List[str]:
        """Resolve S3 file paths with prefix"""
        url = urlparse(self.path)
        bucket_name = url.netloc
        prefix = url.path[1:]
        hook = s3.S3Hook(aws_conn_id=self.conn_id) if self.conn_id else s3.S3Hook()
        prefixes = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        paths = [
            urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes
        ]
        return paths

    @property
    def size(self) -> int:
        return -1
