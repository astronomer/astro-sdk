from typing import List, Optional

import pandas as pd
import smart_open

from astro.constants import FileType
from astro.files.locations import create_file_location
from astro.files.type import create_type_factory


class File:
    """Handle all file operations, and abstract away the details related to location and file types.
    Intended to be used within library.
    """

    def __init__(self, path: str, conn_id: Optional[str] = None):
        """Init file object which represent a single file in local/object stores

        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        """
        self.location = create_file_location(path, conn_id)
        self.type = create_type_factory(path)

    @property
    def path(self):
        return self.location.path

    @property
    def size(self) -> int:
        """
        Return the size (bytes) of the given file.

        :return: File size in bytes
        :rtype: int
        """
        size: int = self.location.size
        return size

    def is_binary(self) -> bool:
        """
        Return a FileType given the filepath. Uses a naive strategy, using the file extension.

        :return: True or False
        :rtype: bool
        """
        result: bool = self.type.name == FileType.PARQUET
        return result

    def write_from_dataframe(self, df: pd.DataFrame) -> None:
        """Write dataframe to all supported files formats

        :param df: pandas dataframe
        """
        with smart_open.open(
            self.path, mode="wb", transport_params=self.location.transport_params
        ) as stream:
            self.type.write_from_dataframe(stream=stream, df=df)

    def read_to_dataframe(
        self, normalize_config: Optional[dict] = None, **kwargs
    ) -> pd.DataFrame:
        """Read file from all supported location and convert them into dataframes

        :param normalize_config: normalize_config: parameters in dict format of pandas json_normalize() function.
        """
        mode = "rb" if self.is_binary() else "r"
        with smart_open.open(
            self.path, mode=mode, transport_params=self.location.transport_params
        ) as stream:
            return self.type.read_to_dataframe(
                stream, normalize_config=normalize_config, **kwargs
            )

    def exists(self) -> bool:
        """Check if the file exists or not"""
        file_exists: bool = self.location.exists()
        return file_exists


def get_files(path_pattern: str, conn_id: Optional[str] = None) -> List[File]:
    """get file objects by resolving path_pattern from local/object stores
    path_pattern can be
    1. local location - glob pattern
    2. s3/gcs location - prefix

    :param path_pattern: path/pattern to a file in the filesystem/Object stores,
    supports glob and prefix pattern for object stores
    :param conn_id: Airflow connection ID
    """
    location = create_file_location(path_pattern, conn_id)
    return [File(path, conn_id) for path in location.paths]
