from __future__ import annotations

from astro.constants import FileLocation
from astro.files.locations.base import BaseFileLocation


class HTTPLocation(BaseFileLocation):
    """Handler http location operations"""

    location_type = FileLocation.HTTP

    @property
    def paths(self) -> list[str]:
        """Resolve patterns in path"""
        return [self.path]

    @property
    def size(self) -> int:
        return -1
