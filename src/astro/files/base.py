import os
import pathlib
from typing import List, Optional

import pandas as pd
import smart_open

from astro.constants import FileType
from astro.files.locations import location_factory
from astro.files.type import type_factory


class File:
    def __init__(self, path: str, conn_id: Optional[str] = None):
        """Init file object which represent a single file in local/object stores

        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        self.location = location_factory(path, conn_id)
        self.type = type_factory(path)

    @property
    def path(self):
        return self.location.path

    def get_size(self) -> int:
        """
        Return the size (bytes) of the given file.

        :return: File size in bytes
        :rtype: int
        """
        path = pathlib.Path(self.path)
        return os.path.getsize(path)

    def is_binary(self) -> bool:
        """
        Return a FileType given the filepath. Uses a naive strategy, using the file extension.

        :return: True or False
        :rtype: bool
        """
        result: bool = self.type.name == FileType.PARQUET
        return result

    def write(self, df: pd.DataFrame) -> None:
        """Write dataframe to all supported files formats
        :param df: pandas dataframe
        """
        with smart_open.open(
            self.path, mode="wb", transport_params=self.location.get_transport_params()
        ) as stream:
            self.type.write(stream=stream, df=df)

    def read(self, normalize_config: Optional[dict] = None, **kwargs) -> pd.DataFrame:
        """Read file from all supported location and convert them into dataframes
        :param normalize_config: normalize_config: parameters in dict format of pandas json_normalize() function.
        """
        mode = "rb" if self.is_binary() else "r"
        with smart_open.open(
            self.path, mode=mode, transport_params=self.location.get_transport_params()
        ) as stream:
            return self.type.read(stream, normalize_config=normalize_config, **kwargs)

    def exists(self) -> bool:
        """Check if the file exists or not"""
        file_exists: bool = self.location.exists()
        return file_exists


def get_files(path_pattern: str, conn_id: Optional[str] = None) -> List[File]:
    """get file objects by resolving path_pattern from local/object stores

    :param path_pattern: path/pattern to a file in the filesystem/Object stores,
    supports glob and prefix pattern for object stores
    :param conn_id: Airflow connection ID
    """
    location = location_factory(path_pattern, conn_id)
    return [File(path, conn_id) for path in location.get_paths()]
