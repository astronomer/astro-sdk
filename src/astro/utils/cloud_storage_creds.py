import os
from urllib import parse

import boto3
from google.cloud.storage import Client


def parse_s3_env_var():
    raw_data = (
        os.environ["AIRFLOW__ASTRO__CONN_AWS_DEFAULT"]
        .replace("%2F", "/")
        .replace("aws://", "")
        .replace("@", "")
        .split(":")
    )
    return [parse.unquote(r) for r in raw_data]


def s3fs_creds():
    # To-do: reuse this method from sql decorator
    """Structure s3fs credentials from Airflow connection.
    s3fs enables pandas to write to s3
    """
    # To-do: clean-up how S3 creds are passed to s3fs

    k, v = parse_s3_env_var()
    session = boto3.Session(
        aws_access_key_id=k,
        aws_secret_access_key=v,
    )
    return dict(client=session.client("s3"))


def gcs_client():
    """
    get GCS credentials for storage.
    """
    client = Client()
    return dict(client=client)
