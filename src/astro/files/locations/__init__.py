from typing import Dict, Optional, Type, Union
from urllib.parse import urlparse

from astro.constants import FileLocation
from astro.files.locations.base import Location
from astro.files.locations.gcs import GS
from astro.files.locations.http import Http
from astro.files.locations.local import Local
from astro.files.locations.s3 import S3


def location_factory(path: str, conn_id: Optional[str] = None) -> Location:
    """Location factory method to generate location class
    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    """
    location_to_object: Dict[FileLocation, Type[Union[S3, GS, Local, Http]]] = {
        FileLocation.LOCAL: Local,
        FileLocation.S3: S3,
        FileLocation.GS: GS,
        FileLocation.HTTP: Http,
        FileLocation.HTTPS: Http,
    }
    return location_to_object[location_type(path)](path, conn_id)


def location_type(path: str) -> FileLocation:
    """Identify where a file is located
    :param path: Path to a file in the filesystem/Object stores
    """
    file_scheme = urlparse(path).scheme
    if file_scheme == "":
        location = FileLocation.LOCAL
    else:
        try:
            location = getattr(FileLocation, file_scheme.upper())
        except (UnboundLocalError, AttributeError):
            raise ValueError(f"Unsupported scheme '{file_scheme}' from path '{path}'")
    return location
