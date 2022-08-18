import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType
from astro.utils.dataframe import convert_columns_names_capitalization


class ParquetFileType(FileType):
    """Concrete implementation to handle Parquet file type"""

    def export_to_dataframe(
        self, stream, columns_names_capitalization="original", **kwargs
    ):
        """read parquet file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        kwargs_copy = dict(kwargs)
        # Pandas `read_parquet` does not support the `nrows` parameter
        kwargs_copy.pop("nrows", None)

        df = pd.read_parquet(stream, **kwargs_copy)
        df = convert_columns_names_capitalization(
            df=df, columns_names_capitalization=columns_names_capitalization
        )
        return df

    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write parquet file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_parquet(stream)

    @property
    def name(self):
        return FileTypeConstants.PARQUET
