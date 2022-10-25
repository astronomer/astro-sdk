from __future__ import annotations

from delta.tables import *
from airflow.exceptions import AirflowException
from airflow.hooks.dbapi import DbApiHook

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    ColumnCapitalization,
    FileLocation,
    LoadExistStrategy,
    MergeConflictStrategy,
)
from astro.databases.base import BaseDatabase
from astro.files import File
from astro.spark.builder import build_spark_session
from astro.spark.table import DeltaTable as AstroDeltaTable
from astro.table import BaseTable, Metadata
from sqlalchemy.sql import ClauseElement
from pyspark.sql.dataframe import DataFrame

class DeltaDatabase(BaseDatabase):
    _create_table_statement: str = "CREATE TABLE IF NOT EXISTS {} USING DELTA AS {} "

    @property
    def sql_type(self):
        return "delta"

    @property
    def hook(self) -> DbApiHook:
        pass

    @property
    def default_metadata(self) -> Metadata:
        return Metadata()

    def create_table_using_native_schema_autodetection(self, table: BaseTable, file: File) -> None:
        pass

    def merge_table(self, source_table: BaseTable, target_table: BaseTable,
                    source_to_target_columns_map: dict[str, str], target_conflict_columns: list[str],
                    if_conflicts: MergeConflictStrategy = "exception") -> None:
        pass

    def schema_exists(self, schema: str) -> bool:
        return True

    def create_schema_if_needed(self, schema: str | None) -> None:
        return None

    def load_file_to_table(
        self,
        input_file: File,
        output_table: BaseTable,
        normalize_config: dict | None = None,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        use_native_support: bool = True,
        native_support_kwargs: dict | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
        enable_native_fallback: bool | None = True,
        **kwargs,
    ):
        """
        Load content of multiple files in output_table.
        Multiple files are sourced from the file path, which can also be path pattern.

        :param input_file: File path and conn_id for object stores
        :param output_table: Table to create
        :param if_exists: Overwrite file if exists
        :param chunk_size: Specify the number of records in each batch to be written at a time
        :param use_native_support: Use native support for data transfer if available on the destination
        :param normalize_config: pandas json_normalize params config
        :param native_support_kwargs: kwargs to be used by method involved in native support flow
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer
        """
        if not isinstance(output_table, AstroDeltaTable):
            raise AirflowException("sdafd")
        configs = {}
        extra_packages = []
        if input_file.location.location_type == FileLocation.S3:
            configs.update(input_file.location.spark_config())
            extra_packages.extend(input_file.location.spark_packages())

        spark = build_spark_session("foo", configs, extra_packages)
        df = spark.read.format("csv").load(input_file.path, header=True, infer_schema=True)
        df.write.format("delta").mode("overwrite" if if_exists == "replace" else "error").save(output_table.path)

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        pass

    def openlineage_dataset_namespace(self) -> str:
        pass

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: BaseTable,
        parameters: dict | None = None,
    ) -> None:
        configs = {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                   "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"}

        extra_packages = []
        spark = build_spark_session("foo", configs, extra_packages)
        statement = self._create_table_statement.format(
            self.get_table_qualified_name(target_table), statement
        )
        self.run_sql(sql=statement, parameters=None, spark_session=spark)

    def run_sql(
        self,
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        **kwargs,
    ):
        configs = {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                   "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"}

        extra_packages = []
        spark = kwargs.get("spark_session") or build_spark_session("foo", configs, extra_packages)
        spark.sql(sql)

    def export_table_to_pandas_dataframe(
        self, source_table: AstroDeltaTable, select_kwargs: dict | None = None
    ) -> DataFrame:
        configs = {"spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
                   "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog"}

        extra_packages = []
        spark = build_spark_session("foo", configs, extra_packages)
        return spark.read.format("delta").load(source_table.path)
