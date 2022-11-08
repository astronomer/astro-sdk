from __future__ import annotations

from airflow.hooks.dbapi import DbApiHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from pyspark.sql.dataframe import DataFrame
from sqlalchemy.sql import ClauseElement

from astro.constants import DEFAULT_CHUNK_SIZE, ColumnCapitalization, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.files import File
from astro.spark.autoloader.autoloader_job import load_file_to_delta
from astro.table import BaseTable, Metadata
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from databricks_cli.sdk.api_client import ApiClient

class DeltaDatabase(BaseDatabase):
    _create_table_statement: str = "CREATE TABLE IF NOT EXISTS {} USING DELTA AS {} "

    def __init__(self, conn_id: str, table: BaseTable | None = None):
        super().__init__(conn_id)
        self.table = table

    def api_client(self):
        conn = DatabricksHook(databricks_conn_id=self.conn_id).get_conn()
        api_client = ApiClient(host=conn.host, token=conn.password)
        return api_client

    def row_count(self, table):
        x = self.run_sql("SELECT COUNT(*) FROM {}".format(self.get_table_qualified_name(table)), handler=lambda x: x.fetchone())
        return list(x[1].asDict().values())[0]  # please for the love of god let's find a better way to do this

    def populate_table_metadata(self, table: BaseTable) -> BaseTable:
        """
        Since SQLite does not have a concept of databases or schemas, we just return the table as is,
        without any modifications.
        """
        table.conn_id = table.conn_id or self.conn_id
        if not table.metadata or table.metadata.is_empty():
            table.metadata = self.table.metadata or self.default_metadata
        return table

    @property
    def sql_type(self):
        return "delta"

    @property
    def hook(self) -> DbApiHook:
        return DatabricksSqlHook(databricks_conn_id=self.conn_id)

    @property
    def default_metadata(self) -> Metadata:
        return Metadata()

    def create_table_using_native_schema_autodetection(self, table: BaseTable, file: File) -> None:
        pass

    def merge_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ) -> None:
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
        load_file_to_delta(
            input_file=input_file,
            delta_table=output_table,
        )

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
        statement = self._create_table_statement.format(
            self.get_table_qualified_name(target_table), statement
        )
        self.run_sql(sql=statement, parameters=None)

    def run_sql(
        self,
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        handler=None,
        **kwargs,
    ):
        hook = DatabricksSqlHook(
            databricks_conn_id=self.conn_id,
        )
        return hook.run(sql, parameters=parameters, handler=handler)

    def export_table_to_pandas_dataframe(
        self, source_table: BaseTable, select_kwargs: dict | None = None
    ) -> DataFrame:
        return self.hook.run(f"SELECT * FROM {source_table.name}", handler=lambda cur: cur.fetchall_arrow().to_pandas())[1]
