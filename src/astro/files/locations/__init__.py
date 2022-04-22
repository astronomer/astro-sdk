from typing import Dict, Optional, Type

from astro.constants import FileLocation
from astro.files.locations.base import Location
from astro.files.locations.gcs import GCS
from astro.files.locations.http import Http
from astro.files.locations.local import Local
from astro.files.locations.s3 import S3


def location_factory(path: str, conn_id: Optional[str] = None) -> Location:
    """Location factory method to generate location class
    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    """
    location_to_object: Dict[FileLocation, Type[Location]] = {
        FileLocation.LOCAL: Local,
        FileLocation.S3: S3,
        FileLocation.GS: GCS,
        FileLocation.HTTP: Http,
        FileLocation.HTTPS: Http,
    }
    filetype: FileLocation = Location.get_location_type(path)
    return location_to_object[filetype](path, conn_id)
