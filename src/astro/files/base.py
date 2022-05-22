from typing import List, Optional, Union

import pandas as pd
import smart_open

from astro.constants import FileType
from astro.files.locations import create_file_location
from astro.files.types import create_file_type


class File:
    """Handle all file operations, and abstract away the details related to location and file types.
    Intended to be used within library.
    """

    template_fields = ("location",)

    def __init__(
        self,
        path: str,
        conn_id: Optional[str] = None,
        filetype: Union[FileType, None] = None,
        normalize_config: Optional[dict] = None,
    ):
        """Init file object which represent a single file in local/object stores

        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        :param filetype: constant to provide an explicit file type
        :param normalize_config: parameters in dict format of pandas json_normalize() function.
        """
        self.location = create_file_location(path, conn_id)
        self.type = create_file_type(
            path=path, filetype=filetype, normalize_config=normalize_config
        )

    @property
    def path(self) -> str:
        return self.location.path

    @property
    def conn_id(self) -> Optional[str]:
        return self.location.conn_id

    @property
    def size(self) -> int:
        """
        Return the size in bytes of the given file.

        :return: File size in bytes
        """
        size: int = self.location.size
        return size

    def is_binary(self) -> bool:
        """
        Return a FileType given the filepath. Uses a naive strategy, using the file extension.

        :return: True or False
        """
        result: bool = self.type.name == FileType.PARQUET
        return result

    def create_from_dataframe(self, df: pd.DataFrame) -> None:
        """Create a file in the desired location using the values of a dataframe.

        :param df: pandas dataframe
        """
        with smart_open.open(
            self.path, mode="wb", transport_params=self.location.transport_params
        ) as stream:
            self.type.create_from_dataframe(stream=stream, df=df)

    def export_to_dataframe(self, **kwargs) -> pd.DataFrame:
        """Read file from all supported location and convert them into dataframes"""
        mode = "rb" if self.is_binary() else "r"
        with smart_open.open(
            self.path, mode=mode, transport_params=self.location.transport_params
        ) as stream:
            return self.type.export_to_dataframe(stream, **kwargs)

    def exists(self) -> bool:
        """Check if the file exists or not"""
        file_exists: bool = self.location.exists()
        return file_exists

    def __repr__(self):
        return (
            f'{self.__class__.__name__}(location="{self.location}",type="{self.type}")'
        )

    def __str__(self):
        return self.location.path


def get_files(
    path_pattern: str,
    conn_id: Optional[str] = None,
    filetype: Union[FileType, None] = None,
    normalize_config: Optional[dict] = None,
) -> List[File]:
    """get file objects by resolving path_pattern from local/object stores
    path_pattern can be
    1. local location - glob pattern
    2. s3/gcs location - prefix

    :param path_pattern: path/pattern to a file in the filesystem/Object stores,
        supports glob and prefix pattern for object stores
    :param conn_id: Airflow connection ID
    :param filetype: constant to provide an explicit file type
    :param normalize_config: parameters in dict format of pandas json_normalize() function.
    """
    location = create_file_location(path_pattern, conn_id)
    return [
        File(
            path=path,
            conn_id=conn_id,
            filetype=filetype,
            normalize_config=normalize_config,
        )
        for path in location.paths
    ]
