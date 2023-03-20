from __future__ import annotations

import io
import pathlib
from typing import cast

import pandas as pd
import smart_open
from attr import define, field

from universal_transfer_operator.constants import FileType as FileTypeConstant
from universal_transfer_operator.datasets.base import Dataset
from universal_transfer_operator.datasets.file.types import create_file_type
from universal_transfer_operator.datasets.file.types.base import FileTypes


@define
class File(Dataset):
    """
    Repersents all file dataset.

    :param path: Path to a file in the filesystem/Object stores
    :param conn_id: Airflow connection ID
    :param filetype: constant to provide an explicit file type
    :param normalize_config: parameters in dict format of pandas json_normalize() function.
    :param is_bytes: is bytes
    """

    path: str = field(default="")
    conn_id: str = field(default="")
    filetype: FileTypeConstant | None = None
    normalize_config: dict | None = None
    is_bytes: bool = False
    uri: str = field(init=False)
    extra: dict = field(init=True, factory=dict)
    is_dataframe: bool = False

    @property
    def location(self):
        from universal_transfer_operator.data_providers import create_dataprovider
        from universal_transfer_operator.data_providers.filesystem.base import BaseFilesystemProviders

        return cast(BaseFilesystemProviders, create_dataprovider(dataset=self))

    @property
    def size(self) -> int:
        """
        Return the size in bytes of the given file.

        :return: File size in bytes
        """
        size: int = self.location.size
        return size

    @property
    def type(self) -> FileTypes:  # noqa: A003
        return create_file_type(
            path=self.path,
            filetype=self.filetype,
            normalize_config=self.normalize_config,
        )

    def is_binary(self) -> bool:
        """
        Return a constants.FileType given the filepath. Uses a native strategy, using the file extension.

        :return: True or False
        """
        read_as_non_binary = {FileTypeConstant.CSV, FileTypeConstant.JSON, FileTypeConstant.NDJSON}
        if self.type in read_as_non_binary:
            return False
        return True

    def is_pattern(self) -> bool:
        """
        Returns True when file path is a pattern(eg. s3://bucket/folder or /folder/sample_* etc)

        :return: True or False
        """
        return not pathlib.PosixPath(self.path).suffix

    def create_from_dataframe(self, df: pd.DataFrame, store_as_dataframe: bool = True) -> None:
        """Create a file in the desired location using the values of a dataframe.

        :param df: pandas dataframe
        :param store_as_dataframe: Whether the data should later be deserialized as a dataframe or as a file containing
            delimited data (e.g. csv, parquet, etc.).
        """
        self.is_dataframe = store_as_dataframe

        with smart_open.open(self.path, mode="wb", transport_params=self.location.transport_params) as stream:
            self.type.create_from_dataframe(stream=stream, df=df)

    def export_to_dataframe(self, **kwargs) -> pd.DataFrame:
        """Read file from all supported location and convert them into dataframes."""
        mode = "rb" if self.is_binary() else "r"
        with smart_open.open(self.path, mode=mode, transport_params=self.location.transport_params) as stream:
            return self.type.export_to_dataframe(stream, **kwargs)

    def _convert_remote_file_to_byte_stream(self) -> io.IOBase:
        """
        Read file from all supported location and convert them into a buffer that can be streamed into other data
        structures.
        Due to noted issues with using smart_open with pandas (like
        https://github.com/RaRe-Technologies/smart_open/issues/524), we create a BytesIO or StringIO buffer
        before exporting to a dataframe. We've found a sizable speed improvement with this optimization

        :returns: an io object that can be streamed into a dataframe (or other object)
        """

        mode = "rb" if self.is_binary() else "r"
        remote_obj_buffer = io.BytesIO() if self.is_binary() else io.StringIO()
        with smart_open.open(self.path, mode=mode, transport_params=self.location.transport_params) as stream:
            remote_obj_buffer.write(stream.read())
        remote_obj_buffer.seek(0)
        return remote_obj_buffer

    def export_to_dataframe_via_byte_stream(self, **kwargs) -> pd.DataFrame:
        """Read files from all supported locations and convert them into dataframes.
        Due to noted issues with using smart_open with pandas (like
        https://github.com/RaRe-Technologies/smart_open/issues/524), we create a BytesIO or StringIO buffer
        before exporting to a dataframe. We've found a sizable speed improvement with this optimization.
        """

        return self.type.export_to_dataframe(self._convert_remote_file_to_byte_stream(), **kwargs)

    def exists(self) -> bool:
        """Check if the file exists or not"""
        file_exists: bool = self.location.exists()
        return file_exists

    def __str__(self) -> str:
        return self.path

    def __eq__(self, other) -> bool:
        if not isinstance(other, self.__class__):
            return NotImplemented
        # Casting self.location to BaseFilesystemProviders, because adding return type on `location` property
        # is causing cyclic dependency issues.
        from universal_transfer_operator.data_providers.filesystem.base import BaseFilesystemProviders

        return (
            cast(BaseFilesystemProviders, self.location) == cast(BaseFilesystemProviders, other.location)
            and self.type == other.type
        )

    def __hash__(self) -> int:
        return hash((self.path, self.conn_id, self.filetype))

    @classmethod
    def from_json(cls, serialized_object: dict):
        filetype = (
            FileTypeConstant(serialized_object["filetype"]) if serialized_object.get("filetype") else None
        )
        return File(
            conn_id=serialized_object["conn_id"],
            path=serialized_object["path"],
            filetype=filetype,
            normalize_config=serialized_object["normalize_config"],
            is_dataframe=serialized_object["is_dataframe"],
        )

    @uri.default
    def _path_to_dataset_uri(self) -> str:
        """Build a URI to be passed to Dataset obj introduced in Airflow 2.4"""
        from urllib.parse import urlencode, urlparse

        parsed_url = urlparse(url=self.path)
        netloc = parsed_url.netloc
        # Local filepaths do not have scheme
        parsed_scheme = parsed_url.scheme or "file"
        scheme = f"astro+{parsed_scheme}"
        extra = {}
        if self.filetype:
            extra["filetype"] = str(self.filetype)

        new_parsed_url = parsed_url._replace(
            netloc=f"{self.conn_id}@{netloc}" if self.conn_id else netloc,
            scheme=scheme,
            query=urlencode(extra),
        )
        return new_parsed_url.geturl()
