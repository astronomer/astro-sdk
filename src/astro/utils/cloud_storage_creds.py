import os
from urllib import parse

from astro.utils.dependencies import BotoSession, GCSClient


def parse_s3_env_var():
    return os.environ["AWS_ACCESS_KEY_ID"], os.environ["AWS_SECRET_ACCESS_KEY"]


def s3fs_creds():
    # To-do: reuse this method from sql decorator
    """Structure s3fs credentials from Airflow connection.
    s3fs enables pandas to write to s3
    """
    # To-do: clean-up how S3 creds are passed to s3fs

    k, v = parse_s3_env_var()
    session = BotoSession(
        aws_access_key_id=k,
        aws_secret_access_key=v,
    )
    return dict(client=session.client("s3"))


def gcs_client():
    """
    get GCS credentials for storage.
    """
    client = GCSClient()
    return dict(client=client)
