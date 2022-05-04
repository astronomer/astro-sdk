import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.type.base import FileType


class Json(FileType):
    def read_to_dataframe(self, stream: io.TextIOWrapper, **kwargs):
        """read json file from one of the supported locations and return dataframe

        :param stream: file stream object
        """
        kwargs.pop("normalize_config")
        return pd.read_json(stream, **kwargs)

    def write_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write json file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_json(stream, orient="records")

    @property
    def name(self):
        return FileTypeConstants.JSON
