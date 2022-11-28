from __future__ import annotations

import pandas
from airflow.hooks.dbapi import DbApiHook
from airflow.providers.databricks.hooks.databricks import DatabricksHook
from airflow.providers.databricks.hooks.databricks_sql import DatabricksSqlHook
from databricks_cli.sdk.api_client import ApiClient
from sqlalchemy.engine import create_engine
from sqlalchemy.engine.base import Engine as SqlAlchemyEngine
from sqlalchemy.sql import ClauseElement

from astro.constants import DEFAULT_CHUNK_SIZE, ColumnCapitalization, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.databricks.utils import load_file_to_delta
from astro.files import File
from astro.table import BaseTable, Metadata


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
        x = self.run_sql(
            f"SELECT COUNT(*) FROM {self.get_table_qualified_name(table)}",
            handler=lambda x: x.fetchone(),
        )
        return list(x[1].asDict().values())[
            0
        ]  # please for the love of god let's find a better way to do this

    def populate_table_metadata(self, table: BaseTable) -> BaseTable:
        """
        Since SQLite does not have a concept of databases or schemas, we just return the table as is,
        without any modifications.
        """
        table.conn_id = table.conn_id or self.conn_id
        if not table.metadata or table.metadata.is_empty():
            if self.table:
                table.metadata = self.table.metadata
            else:
                table.metadata = self.default_metadata
        return table

    @property
    def sql_type(self):
        return "delta"

    @property
    def hook(self) -> DbApiHook:
        return DatabricksSqlHook(databricks_conn_id=self.conn_id)

    @property
    def sqlalchemy_engine(self) -> SqlAlchemyEngine:
        hook = self.hook
        c = hook.databricks_conn
        url = f"databricks+connector://token:{c.password}@{self.hook.host}:443/default"

        return create_engine(url, connect_args=c.extra_dejson)

    def table_exists(self, table: BaseTable) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        from databricks.sql.exc import ServerOperationError
        from sqlalchemy.exc import DatabaseError

        try:
            return super().table_exists(table)
        except DatabaseError as d:
            if isinstance(d.orig, ServerOperationError) and "Table or view not found" in d.orig.message:
                return False
            raise d

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
        return ""

    def openlineage_dataset_namespace(self) -> str:
        return ""

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

    # def table_exists(self, table: BaseTable) -> bool:
    #     from databricks.sql.exc import ServerOperationError
    #     try:
    #         self.hook.run(
    #             f"DESCRIBE TABLE {table.name}", handler=lambda cur: cur.fetchall_arrow().to_pandas()
    #         )
    #         return True
    #     except ServerOperationError as s:
    #         if "Table or view not found" in s.message:
    #             return False
    #         else:
    #             raise s

    def create_table_using_columns(self, table: BaseTable) -> None:
        """
        Create a SQL table using the table columns.

        :param table: The table to be created.
        """
        if not table.columns:
            raise ValueError("To use this method, table.columns must be defined")
        columns = "\n\t".join([f"{c.name} {c.type}," for c in table.columns])
        self.run_sql(
            f"""
CREATE TABLE {table.name}
(
    {columns[:-1]}
);
        """
        )

    def export_table_to_pandas_dataframe(
        self, source_table: BaseTable, select_kwargs: dict | None = None
    ) -> pandas.DataFrame:
        return self.hook.run(
            f"SELECT * FROM {source_table.name}", handler=lambda cur: cur.fetchall_arrow().to_pandas()
        )[1]
