import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.types.base import FileType


class JSONFileType(FileType):
    """Concrete implementation to handle JSON file type"""

    def export_to_dataframe(self, stream: io.TextIOWrapper, **kwargs):
        """read json file from one of the supported locations and return dataframe

        :param stream: file stream object
        """
        return pd.read_json(stream, **kwargs)

    def create_from_dataframe(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write json file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_json(stream, orient="records")

    @property
    def name(self):
        return FileTypeConstants.JSON
