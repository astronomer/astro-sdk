import json
import os
from urllib import parse

from airflow.hooks.base import BaseHook

from astro.utils.dependencies import (
    AwsBaseHook,
    BotoSession,
    GCSClient,
    google_service_account,
)


def parse_s3_env_var():
    raw_data = (
        os.environ["AIRFLOW__ASTRO__CONN_AWS_DEFAULT"]
        .replace("%2F", "/")
        .replace("aws://", "")
        .replace("@", "")
        .split(":")
    )
    return [parse.unquote(r) for r in raw_data]


def s3fs_creds(conn_id=None):
    """Structure s3fs credentials from Airflow connection.
    s3fs enables pandas to write to s3
    """
    if conn_id:
        BaseHook.get_connection(
            conn_id
        )  # This line helps to raise a friendly exception
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
    credentials = None
    if conn_id:
        extra = BaseHook.get_connection(conn_id).extra_dejson
        json_file_path = extra["extra__google_cloud_platform__key_path"]
        with open(json_file_path) as fp:
            json_account_info = json.load(fp)
            credentials = google_service_account.Credentials.from_service_account_info(
                json_account_info
            )
    client = GCSClient(credentials=credentials)
    return dict(client=client)
