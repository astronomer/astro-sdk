import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.type.base import FileType


class CSV(FileType):
    def read_to_dataframe(self, stream, **kwargs) -> pd.DataFrame:
        """read csv file from one of the supported locations and return dataframe

        :param stream: file stream object
        """
        kwargs.pop("normalize_config")
        return pd.read_csv(stream, **kwargs)

    def write_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write csv file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_csv(stream, index=False)

    @property
    def name(self):
        return FileTypeConstants.CSV
