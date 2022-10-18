from __future__ import annotations

from urllib.parse import urlparse

import requests

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
        """Return file size for HTTP location"""
        response = requests.head(self.path, allow_redirects=True)
        return int(response.headers.get("content-length", -1))

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        parsed_url = urlparse(self.path)
        return f"{parsed_url.scheme}://{parsed_url.netloc}"

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return urlparse(self.path).path
