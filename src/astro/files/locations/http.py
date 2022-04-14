from typing import Dict, List, Union

from astro.constants import FileLocation
from astro.files.locations.base import Location


class Http(Location):
    location_type = FileLocation.HTTP

    def get_paths(self) -> List[str]:
        """Resolve patterns in path"""
        return [self.path]

    def get_transport_params(self) -> Union[Dict, None]:
        """Dummy method"""
        return None
