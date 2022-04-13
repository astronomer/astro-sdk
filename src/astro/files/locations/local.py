import glob
import pathlib
from typing import Dict, List, Optional, Union
from urllib.parse import urlparse

from astro.files.locations.base import LocationAbstract


class Local(LocationAbstract):
    """Handler Local file path operations"""

    def get_paths(self, path: str, conn_id: Optional[str]) -> List[str]:
        """Resolve local filepath
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        url = urlparse(path)
        path_object = pathlib.Path(url.path)
        if path_object.is_dir():
            paths = [str(filepath) for filepath in path_object.rglob("*")]
        else:
            paths = glob.glob(url.path)
        return paths

    def get_transport_params(
        self, path: str, conn_id: Optional[str]
    ) -> Union[Dict, None]:
        """Dummy method
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        return None
