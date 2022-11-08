from __future__ import annotations

import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType
from astro.utils.dataframe import convert_columns_names_capitalization


class ParquetFileType(FileType):
    """Concrete implementation to handle Parquet file type"""

    def export_to_dataframe(self, stream, columns_names_capitalization="original", **kwargs):
        """read parquet file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        kwargs_copy = dict(kwargs)
        # Pandas `read_parquet` does not support the `nrows` parameter
        kwargs_copy.pop("nrows", None)

        byte_io_buffer = self._convert_remote_file_to_byte_stream(stream)

        df = pd.read_parquet(byte_io_buffer, **kwargs_copy)
        df = convert_columns_names_capitalization(
            df=df, columns_names_capitalization=columns_names_capitalization
        )
        return df

    @staticmethod
    def _convert_remote_file_to_byte_stream(stream) -> io.IOBase:
        """
        Convert file stream into a buffer that can be streamed into other data
        structures.
        Due to noted issues with using parquet files with smart_open+pandas (like
        https://github.com/RaRe-Technologies/smart_open/issues/524), we create a BytesIO buffer
        before exporting to a dataframe. We've found a sizable speed improvement with this optimization
        Returns: an io object that can be streamed into a dataframe (or other object)
        """
        remote_obj_buffer = io.BytesIO()
        remote_obj_buffer.write(stream.read())
        remote_obj_buffer.seek(0)
        return remote_obj_buffer

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:  # skipcq PYL-R0201
        """Write parquet file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_parquet(stream)

    @property
    def name(self):
        return FileTypeConstants.PARQUET
