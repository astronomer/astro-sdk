import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType


class CSVFileType(FileType):
    """Concrete implementation to handle CSV file type"""

    def export_to_dataframe(self, stream, **kwargs) -> pd.DataFrame:
        """read csv file from one of the supported locations and return dataframe

        :param stream: file stream object
        """
        return pd.read_csv(stream, **kwargs)

    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write csv file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_csv(stream, index=False)

    @property
    def name(self):
        return FileTypeConstants.CSV
