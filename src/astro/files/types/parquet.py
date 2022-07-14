import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType


class ParquetFileType(FileType):
    """Concrete implementation to handle Parquet file type"""

    def export_to_dataframe(self, stream, **kwargs):
        """read parquet file from one of the supported locations and return dataframe

        :param stream: file stream object
        """
        return pd.read_parquet(stream, **kwargs)

    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write parquet file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_parquet(stream)

    @property
    def name(self):
        return FileTypeConstants.PARQUET
