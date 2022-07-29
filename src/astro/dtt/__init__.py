from typing import List

from astro.files.locations import create_file_location


def get_file_list(path: str, conn_id: str) -> List[str]:
    """
    List the file path from the filesystem storage based on given path pattern

    Supported filesystem: Local, HTTP, S3, GCS

    :param conn_id: Airflow connection id
    :param path: Path pattern to the file in the filesystem/Object stores
    """
    location = create_file_location(path, conn_id)
    return location.paths
