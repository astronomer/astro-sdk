import os
from typing import Dict, List, Optional, Tuple
from urllib.parse import urlparse, urlunparse

from airflow.hooks.base import BaseHook

from astro.files.locations.base import LocationAbstract
from astro.utils.dependencies import AwsBaseHook, BotoSession, s3


class S3(LocationAbstract):
    """Handler S3 object store operations"""

    @staticmethod
    def _parse_s3_env_var() -> Tuple[str, str]:
        """Return S3 ID/KEY pair from environment vars"""
        return os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]

    def get_transport_params(self, path: str, conn_id: Optional[str]) -> Dict:
        """Structure s3fs credentials from Airflow connection.
        s3fs enables pandas to write to s3
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        if conn_id:
            # The following line raises a friendly exception
            BaseHook.get_connection(conn_id)
            aws_hook = AwsBaseHook(conn_id, client_type="S3")
            session = aws_hook.get_session()  # type: ignore
        else:
            key, secret = S3._parse_s3_env_var()
            session = BotoSession(
                aws_access_key_id=key,
                aws_secret_access_key=secret,
            )
        return {"client": session.client("s3")}

    def get_paths(self, path: str, conn_id: Optional[str]) -> List[str]:
        """Resolve S3 file paths with prefix
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        url = urlparse(path)
        bucket_name = url.netloc
        prefix = url.path[1:]
        hook = s3.S3Hook(aws_conn_id=conn_id) if conn_id else s3.S3Hook()
        prefixes = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        paths = [
            urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes
        ]
        return paths
