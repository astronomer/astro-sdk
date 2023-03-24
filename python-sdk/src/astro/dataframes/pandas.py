from __future__ import annotations

import logging
from typing import ClassVar

from pandas import DataFrame, read_json

from astro import settings
from astro.exceptions import AstroSDKConfigError

logger = logging.getLogger(__name__)


class PandasDataframe(DataFrame):
    """Pandas-compatible dataframe class that can be serialized and deserialized into XCom by Airflow 2.5"""

    version: ClassVar[int] = 1

    def serialize(self):
        # Store in the metadata DB if Dataframe < 100 kb
        df_size = self.memory_usage(deep=True).sum()
        if df_size < (settings.MAX_DATAFRAME_MEMORY_FOR_XCOM_DB * 1024):
            logger.info("Dataframe size: %s bytes. Storing it in Airflow's metadata DB", df_size)
            return {"data": self.to_json()}
        elif settings.DATAFRAME_STORAGE_CONN_ID is not None:
            # Avoid cyclic dependency
            from astro.utils.dataframe import convert_dataframe_to_file

            logger.info(
                "Dataframe size: %s bytes. Storing it in Remote Storage (conn_id: %s | URL: %s)",
                df_size,
                settings.DATAFRAME_STORAGE_CONN_ID,
                settings.DATAFRAME_STORAGE_URL,
            )
            return convert_dataframe_to_file(self).to_json()

        raise AstroSDKConfigError(
            "Dataframe size exceeds allowed limit for storing in Airflow's metadata DB. "
            "Airflow config variable AIRFLOW__ASTRO_SDK__XCOM_STORAGE_CONN_ID needs to "
            "be set for remote storage of the dataframe."
        )

    @staticmethod
    def deserialize(data: dict, version: int):
        if version > 1:
            raise TypeError(f"version > {PandasDataframe.version}")
        if isinstance(data, dict) and data.get("class", "") == "File":
            # Avoid cyclic dependency
            from astro.files import File

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
