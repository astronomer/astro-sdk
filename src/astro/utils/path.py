import glob
import os
from urllib.parse import urlparse, urlunparse

from astro.utils.cloud_storage_creds import gcs_client, s3fs_creds
from astro.utils.dependencies import gcs, s3


def validate_path(path):
    """
    Check if the give path is either a valid URI or a local file.

    :param path: Either local filesystem path or remote URI
    :type path: str
    :return: if fails, raise `ValueError`
    :rtype: `ValueError`
    """
    result = urlparse(path)
    if not (all([result.scheme, result.netloc]) or os.path.isfile(path)):
        raise ValueError(f"Invalid path: {path}")


def get_transport_params(path, conn_id):
    """
    Given a filesystem, GCS or S3 prefix, return files within that path.

    :param path: Either local filesystem path or remote URI
    :param conn_id: Airflow connection ID, if connecting to S3 or GCS
    :type path: str
    :type conn_id: str
    :return: A dictionary containing necessary credentials to access the file, to be used with open_smart
    :rtype: Dict
    """
    file_scheme = urlparse(path).scheme
    params = None
    if file_scheme == "s3":
        params = s3fs_creds(conn_id)
    elif file_scheme == "s3":
        params = gcs_client(conn_id)
    return params


def get_paths(path, conn_id=None):
    """
    Given a filesystem, GCS or S3 prefix, return files within that path.

    :param path: Either local filesystem path or remote URI
    :param conn_id: Airflow connection ID, if connecting to S3 or GCS
    :type path: str
    :type conn_id: str
    :return: A list with URIs or local paths
    :rtype: List(str)
    """
    url = urlparse(path)
    file_location = url.scheme
    paths = [path]
    if file_location == "":
        paths = glob.glob(url.path)
    else:
        bucket_name = url.netloc
        prefix = url.path
        if file_location == "gs":
            hook = gcs.GCSHook(gcp_conn_id=conn_id) if conn_id else gcs.GCSHook()
            prefixes = hook.list(bucket_name=bucket_name, prefix=prefix)
        elif file_location == "s3":
            hook = s3.S3Hook(aws_conn_id=conn_id) if conn_id else s3.S3Hook()
            prefixes = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        else:
            raise ValueError(f"Unsupported file scheme {file_location} for file {path}")
        paths = [
            urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes
        ]
    return paths
