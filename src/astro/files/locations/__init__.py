import os
from typing import Dict, List, Optional, Type, Union
from urllib.parse import urlparse

from astro.constants import FileLocation
from astro.files.locations.gcs import GS
from astro.files.locations.local import Local
from astro.files.locations.s3 import S3


class Location:
    """Generic location class"""

    def __init__(self, path: str, conn_id: Optional[str]):
        """Generic location constructor
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        if Location.valid_path(path):
            self.path = path
            self.conn_id = conn_id
            self.type_object = self.get_location_type_object()
        else:
            raise ValueError(f"Invalid path: '{path}'")

    def get_location_type_object(self) -> Union[S3, GS, Local]:
        """Get location type based on 'path' of file"""
        location: FileLocation = Location.get_location(self.path)
        location_to_object: Dict[FileLocation, Type[Union[S3, GS, Local]]] = {
            FileLocation.LOCAL: Local,
            FileLocation.S3: S3,
            FileLocation.GS: GS,
        }
        return location_to_object[location]()

    @property
    def transport_params(self) -> Dict:
        """Get credentials required by smart open to access files"""
        return self.type_object.get_transport_params(self.path, self.conn_id)

    @property
    def paths(self) -> List[str]:
        """Resolve patterns in path"""
        return self.type_object.get_paths(self.path, self.conn_id)

    @staticmethod
    def get_location(path: str) -> FileLocation:
        """
        Identify where a file is located
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

    @staticmethod
    def valid_path(path: str) -> bool:
        """
        Check if the give path is either a valid URI or a local file
        :param path: Either local filesystem path or remote URI
        """
        result = urlparse(path)
        if not (all([result.scheme, result.netloc]) or os.path.isfile(path)):
            return False
        return True
