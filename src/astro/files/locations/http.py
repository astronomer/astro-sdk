from typing import List

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class HttpLocation(BaseFileLocation):
    """Handler http location operations"""

    location_type = FileLocation.HTTP

    def get_paths(self) -> List[str]:
        """Resolve patterns in path"""
        return [self.path]
