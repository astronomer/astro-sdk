from __future__ import annotations

from astro.files import File
from astro.spark.builder import build_spark_session
from pyspark.sql.dataframe import DataFrame
import pandas
import random
import string


class DeltaFile(File):
    spark_configs = {}

    def __init__(self, spark_configs=None, **kwargs):
        self.spark_configs = spark_configs
        super(DeltaFile, self).__init__(**kwargs)

    def export_to_dataframe(self, **kwargs) -> DataFrame:
        configs = {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                   "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"}

        extra_packages = []
        spark = build_spark_session("foo", configs, extra_packages)
        return spark.read.format("delta").load(self.path)

    def create_from_dataframe(self, df: DataFrame | pandas.DataFrame, store_as_dataframe: bool = True) -> None:
        """Create a file in the desired location using the values of a dataframe.

        :param store_as_dataframe: Whether the data should later be deserialized as a dataframe or as a file containing
            delimited data (e.g. csv, parquet, etc.).
        :param df: pandas dataframe
        """
        if isinstance(df, pandas.DataFrame):
            configs = {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                       "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"}

            extra_packages = []
            spark = build_spark_session("foo", configs, extra_packages)
            df = spark.createDataFrame(df)
        self.is_dataframe = store_as_dataframe
        df.write.format("delta").mode("overwrite").save(self.path)

    def to_json(self):
        self.log.debug("converting file %s into json", self.path)
        return {
            "class": "DeltaFile",
            "conn_id": self.conn_id,
            "path": self.path,
            "normalize_config": self.normalize_config,
            "is_dataframe": self.is_dataframe,
        }

    @classmethod
    def from_json(cls, serialized_object: dict):
        return DeltaFile(
            conn_id=serialized_object["conn_id"],
            path=serialized_object["path"],
            filetype=None,
            normalize_config=serialized_object["normalize_config"],
            is_dataframe=serialized_object["is_dataframe"],
        )



def convert_dataframe_to_file(df: DataFrame) -> File:
    """
    Passes a dataframe into a File using parquet as an efficient storage format. This allows us to use
    Json as a storage method without filling the metadata database. the values for conn_id and bucket path can
    be found in the airflow.cfg as follows:

    [astro]
    dataframe_storage_conn_id=...
    dataframe_storage_url=///
    :param df: Dataframe to convert to file
    :return: File object with reference to stored dataframe file
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(64)
    )

    # importing here to prevent circular imports
    from astro.files import File

    file = DeltaFile(
        path="/tmp/delta-storage-" + unique_id,
        is_dataframe=True,
    )
    file.create_from_dataframe(df)
    return file
