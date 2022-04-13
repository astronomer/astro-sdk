from typing import Dict, List, Optional, Union

from astro.files.locations.base import LocationAbstract


class Http(LocationAbstract):
    def get_paths(self, path: str, conn_id: Optional[str]) -> List[str]:
        """Resolve patterns in path
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        return [path]

    def get_transport_params(
        self, path: str, conn_id: Optional[str]
    ) -> Union[Dict, None]:
        """Dummy method
        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        return None
