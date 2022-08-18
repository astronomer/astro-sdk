import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType
from astro.utils.dataframe import convert_columns_names_capitalization


class JSONFileType(FileType):
    """Concrete implementation to handle JSON file type"""

    def export_to_dataframe(
        self,
        stream: io.TextIOWrapper,
        columns_names_capitalization="original",
        **kwargs
    ) -> pd.DataFrame:
        """read json file from one of the supported locations and return dataframe

        :param stream: file stream object
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        kwargs_copy = dict(kwargs)
        # Pandas `read_json` does not support the `nrows` parameter unless we're using NDJSON
        kwargs_copy.pop("nrows", None)
        df = pd.read_json(stream, **kwargs_copy)
        df = convert_columns_names_capitalization(
            df=df, columns_names_capitalization=columns_names_capitalization
        )
        return df

    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write json file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_json(stream, orient="records")

    @property
    def name(self):
        return FileTypeConstants.JSON
