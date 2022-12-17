from __future__ import annotations

import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.dataframes.load_options import PandasLoadOptions
from astro.dataframes.pandas import PandasDataframe
from astro.files.types.base import FileType
from astro.utils.dataframe import convert_columns_names_capitalization


class CSVFileType(FileType):
    """Concrete implementation to handle CSV file type"""

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def export_to_dataframe(
        self,
        stream,
        pandas_options: PandasLoadOptions | None = None,
        columns_names_capitalization="original",
        **kwargs,
    ) -> pd.DataFrame:  # skipcq PYL-R0201
        """read csv file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param pandas_options: Pandas option to pass to the Pandas lib while reading csv
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        if pandas_options is not None:
            kwargs.update(pandas_options.to_dict)
        df = pd.read_csv(stream, **kwargs)
        df = convert_columns_names_capitalization(
            df=df, columns_names_capitalization=columns_names_capitalization
        )
        return PandasDataframe.from_pandas_df(df)

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:  # skipcq PYL-R0201
        """Write csv file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_csv(stream, index=False)

    @property
    def name(self):
        return FileTypeConstants.CSV
