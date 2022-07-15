import glob
import os
import pathlib
from typing import List
from urllib.parse import urlparse

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class LocalLocation(BaseFileLocation):
    """Handler Local file path operations"""

    location_type = FileLocation.LOCAL

    @property
    def paths(self) -> List[str]:
        """Resolve local filepath"""
        url = urlparse(self.path)
        path_object = pathlib.Path(url.path)
        if path_object.is_dir():
            paths = [str(filepath) for filepath in path_object.rglob("*")]
        else:
            paths = glob.glob(url.path)
        return paths

    @property
    def size(self) -> int:
        """Return the size in bytes of the given file.

        :return: File size in bytes
        """
        path = pathlib.Path(self.path)
        return os.path.getsize(path)
