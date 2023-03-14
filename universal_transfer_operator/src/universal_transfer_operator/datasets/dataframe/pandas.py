from __future__ import annotations

import logging
import random
import string
from typing import TYPE_CHECKING, ClassVar

import pandas as pd
from pandas import DataFrame, read_json

from universal_transfer_operator import settings
from universal_transfer_operator.constants import ColumnCapitalization, FileType

if TYPE_CHECKING:
    from universal_transfer_operator.datasets.file.base import File

logger = logging.getLogger(__name__)


def convert_dataframe_to_file(df: pd.DataFrame) -> File:
    """
    Passes a dataframe into a File using parquet as an efficient storage format. This allows us to use
    Json as a storage method without filling the metadata database. the values for conn_id and bucket path can
    be found in the airflow.cfg as follows:

    [universal_transfer_operator]
    dataframe_storage_conn_id=...
    dataframe_storage_url=///
    :param df: Dataframe to convert to file
    :return: File object with reference to stored dataframe file
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(64)
    )

    file = File(
        path=settings.DATAFRAME_STORAGE_URL + "/" + unique_id + ".parquet",
        conn_id=settings.DATAFRAME_STORAGE_CONN_ID,
        filetype=FileType.PARQUET,
    )
    file.create_from_dataframe(df)
    return file


class PandasDataframe(DataFrame):
    """Pandas-compatible dataframe class that can be serialized and deserialized into XCom by Airflow 2.5"""

    version: ClassVar[int] = 1

    def serialize(self):
        # Store in the metadata DB if Dataframe < 100 kb
        df_size = self.memory_usage(deep=True).sum()
        if df_size < (settings.MAX_DATAFRAME_MEMORY_FOR_XCOM_DB * 1024):
            logger.info("Dataframe size: %s bytes. Storing it in Airflow's metadata DB", df_size)
            return {"data": self.to_json()}
        else:
            logger.info(
                "Dataframe size: %s bytes. Storing it in Remote Storage (conn_id: %s | URL: %s)",
                df_size,
                settings.DATAFRAME_STORAGE_CONN_ID,
                settings.DATAFRAME_STORAGE_URL,
            )
            return convert_dataframe_to_file(self).to_json()

    @staticmethod
    def deserialize(data: dict, version: int):
        if version > 1:
            raise TypeError(f"version > {PandasDataframe.version}")
        if isinstance(data, dict) and data.get("class", "") == "File":
            file = File.from_json(data)
            if file.is_dataframe:
                logger.info("Retrieving file from %s using %s conn_id ", file.path, file.conn_id)
                return file.export_to_dataframe()
            return file
        return PandasDataframe.from_pandas_df(read_json(data["data"]))

    @classmethod
    def from_pandas_df(cls, df: DataFrame) -> DataFrame | PandasDataframe:
        if not settings.NEED_CUSTOM_SERIALIZATION:
            return df
        return cls(df)


def convert_columns_names_capitalization(
    df: pd.DataFrame, columns_names_capitalization: ColumnCapitalization
):
    """
    Convert cols of a dataframe to required case. Options - lower/Upper

    :param df: dataframe whose cols will be altered
    :param columns_names_capitalization: String Literal with possible values - lower/Upper
    """
    if isinstance(df, pd.DataFrame):
        df = PandasDataframe.from_pandas_df(df)
        if columns_names_capitalization == "lower":
            df.columns = [col_label.lower() for col_label in df.columns]  # skipcq: PYL-W0201
        elif columns_names_capitalization == "upper":
            df.columns = [col_label.upper() for col_label in df.columns]  # skipcq: PYL-W0201

    return df
