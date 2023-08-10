from __future__ import annotations

import io

import pandas as pd

from astro.dataframes.load_options import PandasLoadOptions
from astro.dataframes.pandas import PandasDataframe
from astro.files.types.base import FileType
from astro.utils.dataframe import convert_columns_names_capitalization


class ExcelFileType(FileType):
    """Concrete implementation to handle Excel file type"""

    LOAD_OPTIONS_CLASS_NAME = ("PandasLoadOptions",)

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def export_to_dataframe(
        self,
        stream,
        columns_names_capitalization="original",
        **kwargs,
    ) -> pd.DataFrame:  # skipcq PYL-R0201
        """read Excel file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        if isinstance(self.load_options, PandasLoadOptions):
            kwargs = self.load_options.populate_kwargs(kwargs)
        df = pd.read_excel(stream, **kwargs)
        df = convert_columns_names_capitalization(
            df=df, columns_names_capitalization=columns_names_capitalization
        )
        return PandasDataframe.from_pandas_df(df)

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:  # skipcq PYL-R0201
        """Write Excel file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_excel(stream, index=False)
