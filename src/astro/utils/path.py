import glob
import os
import pathlib
from urllib.parse import urlparse, urlunparse

from astro.constants import FileLocation
from astro.utils.cloud_storage_creds import gcs_client, s3fs_creds
from astro.utils.dependencies import gcs, s3


def get_location(path):
    """
    Identify where a file is located.

    :param path: Path to a file in the filesystem
    :type path: str
    :return: Location of the file
    :rtype: astro.constants.FileLocation (enum) constant
    """
    file_scheme = urlparse(path).scheme
    if file_scheme == "":
        location = FileLocation.LOCAL
    else:
        try:
            location = getattr(FileLocation, file_scheme.upper())
        except (UnboundLocalError, AttributeError):
            raise ValueError(
                f"Unsupported scheme '{file_scheme}' from path '{path}'"
            )  # TODO: Use string interpolation as opposed to fstring
    return location


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
        raise ValueError(f"Invalid path: '{path}'")


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
    location = get_location(path)
    params = None
    if location == FileLocation.S3:
        params = s3fs_creds(conn_id)
    elif location == FileLocation.GS:
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
    location = get_location(path)
    if location == FileLocation.LOCAL:
        path = pathlib.Path(url.path)
        if path.is_dir():
            paths = [str(filepath) for filepath in path.rglob("*")]
        else:
            paths = glob.glob(url.path)
    elif location in (FileLocation.HTTP, FileLocation.HTTPS):
        paths = [path]
    else:
        bucket_name = url.netloc
        prefix = url.path[1:]
        if location == FileLocation.GS:
            hook = gcs.GCSHook(gcp_conn_id=conn_id) if conn_id else gcs.GCSHook()
            prefixes = hook.list(bucket_name=bucket_name, prefix=prefix)
        else:  # location == FileLocation.S3:
            hook = s3.S3Hook(aws_conn_id=conn_id) if conn_id else s3.S3Hook()
            prefixes = hook.list_keys(bucket_name=bucket_name, prefix=prefix)
        paths = [
            urlunparse((url.scheme, url.netloc, keys, "", "", "")) for keys in prefixes
        ]
    return paths


def is_local(path):
    """
    Check if a path is local to the filesystem or not.

    :param path: Either a filesystem path or remote URI
    :type path: str
    :return: True or False
    :rtype: bool
    """
    file_location = get_location(path)
    return file_location == FileLocation.LOCAL
