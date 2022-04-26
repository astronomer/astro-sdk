import io

import pandas as pd

from astro.constants import FileType as FileTypeConstants
from astro.files.type.base import FileType


class Parquet(FileType):
    def read(self, stream, **kwargs):
        """read parquet file from one of the supported locations and return dataframe

        :param stream: file stream object
        """
        kwargs.pop("normalize_config")
        # parquet_file = ParquetFile(stream)
        # first_rows = next(parquet_file.iter_batches(batch_size=LOAD_COLUMN_AUTO_DETECT_ROWS))
        # return pa.Table.from_batches([first_rows]).to_pandas()
        return pd.read_parquet(stream, **kwargs)

    def write(self, df: pd.DataFrame, stream: io.TextIOWrapper) -> None:
        """Write parquet file to one of the supported locations

        :param df: pandas dataframe
        :param stream: file stream object
        """
        df.to_parquet(stream)

    @property
    def name(self):
        return FileTypeConstants.PARQUET
