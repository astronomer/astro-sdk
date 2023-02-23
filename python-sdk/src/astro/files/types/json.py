from __future__ import annotations

import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.dataframes.load_options import PandasLoadOptions
from astro.dataframes.pandas import PandasDataframe
from astro.files.types.base import FileType
from astro.utils.dataframe import convert_columns_names_capitalization


class JSONFileType(FileType):
    """Concrete implementation to handle JSON file type"""

    LOAD_OPTIONS_CLASS_NAME = ("PandasJsonLoadOptions", "PandasLoadOptions")

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def export_to_dataframe(
        self,
        stream: io.TextIOWrapper,
        columns_names_capitalization="original",
        **kwargs,
    ) -> pd.DataFrame:  # skipcq PYL-R0201
        """read json file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param load_options: Pandas option to pass to the Pandas lib while reading json
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        kwargs_copy = dict(kwargs)
        # Pandas `read_json` does not support the `nrows` parameter unless we're using NDJSON
        kwargs_copy.pop("nrows", None)
        if isinstance(self.load_options, PandasLoadOptions):
            kwargs_copy = self.load_options.populate_kwargs(kwargs)
        df = pd.read_json(stream, **kwargs_copy)
        df = convert_columns_names_capitalization(
            df=df, columns_names_capitalization=columns_names_capitalization
        )
        return PandasDataframe.from_pandas_df(df)

    # We need skipcq because it's a method overloading so we don't want to make it a static method
    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:  # skipcq PYL-R0201
        """Write json file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_json(stream, orient="records")

    @property
    def name(self):
        return FileTypeConstants.JSON
