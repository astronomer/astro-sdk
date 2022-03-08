import json
import os
from urllib import parse

from airflow.hooks.base import BaseHook

from astro.utils.dependencies import (
    AwsBaseHook,
    BotoSession,
    GCSClient,
    GCSHook,
    google_service_account,
)


def parse_s3_env_var():
    return os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]


def s3fs_creds(conn_id=None):
    """Structure s3fs credentials from Airflow connection.
    s3fs enables pandas to write to s3
    """
    if conn_id:
        # The following line raises a friendly exception
        BaseHook.get_connection(conn_id)
        aws_hook = AwsBaseHook(conn_id, client_type="S3")
        session = aws_hook.get_session()
    else:
        key, secret = parse_s3_env_var()
        session = BotoSession(
            aws_access_key_id=key,
            aws_secret_access_key=secret,
        )
    return dict(client=session.client("s3"))


def gcs_client(conn_id=None):
    """
    get GCS credentials for storage.
    """
    if conn_id:
        gcs_hook = GCSHook(conn_id)
        client = gcs_hook.get_conn()
    else:
        client = GCSClient()

    return dict(client=client)
