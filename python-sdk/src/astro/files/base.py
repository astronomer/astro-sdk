from __future__ import annotations

import io

import pandas as pd
import smart_open

from astro import constants
from astro.files.locations import create_file_location
from astro.files.locations.base import BaseFileLocation
from astro.files.types import create_file_type, get_filetype
from astro.files.types.pattern import PatternFileType


class File:  # skipcq: PYL-W1641
    """
    Handle all file operations, and abstract away the details related to location and file types.
    Intended to be used within library.
    """

    template_fields = ("location",)

    def __init__(
        self,
        path: str,
        conn_id: str | None = None,
        filetype: constants.FileType | None = None,
        normalize_config: dict | None = None,
    ):
        """Init file object which represent a single file in local/object stores

        :param path: Path to a file in the filesystem/Object stores
        :param conn_id: Airflow connection ID
        :param filetype: constant to provide an explicit file type
        :param normalize_config: parameters in dict format of pandas json_normalize() function.
        """
        self.location: BaseFileLocation = create_file_location(path, conn_id)
        self.normalize_config = normalize_config
        self.provided_filetype = filetype
        self.type = create_file_type(
            path=path, filetype=filetype, normalize_config=normalize_config
        )

    @property
    def path(self) -> str:
        return self.location.path

    @property
    def conn_id(self) -> str | None:
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
        Return a constants.FileType given the filepath. Uses a naive strategy, using the file extension.

        :return: True or False
        """

        result: bool = self.type.name == constants.FileType.PARQUET
        return result

    def create_from_dataframe(self, df: pd.DataFrame) -> None:
        """Create a file in the desired location using the values of a dataframe.

        :param df: pandas dataframe
        """
        with smart_open.open(
            self.path, mode="wb", transport_params=self.location.transport_params
        ) as stream:
            self.type.create_from_dataframe(stream=stream, df=df)

    def _convert_remote_file_to_byte_stream(self) -> io.IOBase:
        """
        Read file from all supported location and convert them into a buffer that can be streamed into other data
        structures.

        Due to noted issues with using smart_open with pandas (like
        https://github.com/RaRe-Technologies/smart_open/issues/524), we create a BytesIO or StringIO buffer
        before exporting to a dataframe. We've found a sizable speed improvement with this optimizat

        Returns: an io object that can be streamed into a dataframe (or other object)

        """
        mode = "rb" if self.is_binary() else "r"
        remote_obj_buffer = io.BytesIO() if self.is_binary() else io.StringIO()
        with smart_open.open(
            self.path, mode=mode, transport_params=self.location.transport_params
        ) as stream:
            remote_obj_buffer.write(stream.read())
        remote_obj_buffer.seek(0)
        return remote_obj_buffer

    def export_to_dataframe(self, **kwargs) -> pd.DataFrame:
        """Read file from all supported location and convert them into dataframes.

        Due to noted issues with using smart_open with pandas (like
        https://github.com/RaRe-Technologies/smart_open/issues/524), we create a BytesIO or StringIO buffer
        before exporting to a dataframe. We've found a sizable speed improvement with this optimization.
        """
        return self.type.export_to_dataframe(
            self._convert_remote_file_to_byte_stream(), **kwargs
        )

    def exists(self) -> bool:
        """Check if the file exists or not"""
        file_exists: bool = self.location.exists()
        return file_exists

    def __repr__(self) -> str:
        return (
            f'{self.__class__.__name__}(location="{self.location}",type="{self.type}")'
        )

    def __str__(self) -> str:
        return self.location.path  # type: ignore

    def __eq__(self, other):
        return self.location == other.location and self.type == other.type

    def __iter__(self):
        """
        Returns an iterable based on file pattern
        """
        location = create_file_location(self.path, self.conn_id)

        self._files = []  # skipcq : PYL-W0201
        self._file_counter = 0  # skipcq : PYL-W0201
        for path in location.paths:
            if path.endswith("/"):
                continue

            self._files.append(
                File(
                    path=path,
                    conn_id=self.conn_id,
                    filetype=get_filetype(path)
                    if isinstance(self.type, PatternFileType)
                    else self.provided_filetype,
                    normalize_config=self.normalize_config,
                )
            )
        if len(self._files) == 0:
            raise ValueError(f"File(s) not found for path/pattern '{self.path}'")

        return self

    def __next__(self):
        if self._file_counter >= len(self._files):
            raise StopIteration

        file_obj = self._files[self._file_counter]
        self._file_counter += 1
        return file_obj

    def get_first(self):
        """
        Returns the first item in iterator
        """
        return next(iter(self))
