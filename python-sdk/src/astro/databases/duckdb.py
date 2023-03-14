from __future__ import annotations

import socket

import sqlalchemy
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from sqlalchemy import MetaData as SqlaMetaData
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.constants import MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.options import LoadOptions
from astro.table import BaseTable, Metadata
from astro.utils.compat.functools import cached_property

DEFAULT_CONN_ID = DuckDBHook.default_conn_name


class DuckdbDatabase(BaseDatabase):
    """Handle interactions with Duckdb databases."""

    def __init__(
        self,
        conn_id: str = DEFAULT_CONN_ID,
        table: BaseTable | None = None,
        load_options: LoadOptions | None = None,
    ):
        super().__init__(conn_id)
        self.table = table
        self.load_options = load_options

    @property
    def sql_type(self) -> str:
        return "duckdb"

    # We are caching this property to persist the DuckDB in-memory connection, to avoid
    # the problem described in
    # https://github.com/astronomer/astro-sdk/issues/1831
    @cached_property
    def connection(self) -> sqlalchemy.engine.base.Connection:  # skipcq PYL-W0236
        """Return a Sqlalchemy connection object for the given database."""
        return self.sqlalchemy_engine.connect()

    @cached_property
    def hook(self) -> DuckDBHook:
        """Retrieve Airflow hook to interface with the DuckDB database."""
        return DuckDBHook(duckdb_conn_id=self.conn_id)

    @property
    def default_metadata(self) -> Metadata:
        """Since Duckdb does not use Metadata, we return an empty Metadata instances."""
        return Metadata()

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    @staticmethod
    def get_table_qualified_name(table: BaseTable) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        return str(table.name)

    def populate_table_metadata(self, table: BaseTable) -> BaseTable:
        """
        Since Duckdb does not have a concept of databases or schemas, we just return the table as is,
        without any modifications.
        """
        table.conn_id = table.conn_id or self.conn_id
        return table

    def create_schema_if_needed(self, schema: str | None) -> None:
        """
        Since Duckdb does not have schemas, we do not need to set a schema here.
        """

    @staticmethod
    def get_merge_initialization_query(parameters: tuple) -> str:
        """
        Handles database-specific logic to handle index for DuckDB.
        """
        return "CREATE UNIQUE INDEX merge_index ON {{table}}(%s)" % ",".join(parameters)  # skipcq PYL-C0209

    def merge_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ) -> None:
        """
        Merge the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be merged to the target_table
        :param target_table: Contains the destination table in which the rows will be merged
        :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names
        :param target_conflict_columns: List of cols where we expect to have a conflict while combining
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """
        statement = "INSERT INTO {main_table} ({target_columns}) SELECT {append_columns} FROM {source_table} Where true"
        if if_conflicts == "ignore":
            statement += " ON CONFLICT ({merge_keys}) DO NOTHING"
        elif if_conflicts == "update":
            statement += " ON CONFLICT ({merge_keys}) DO UPDATE SET {update_statements}"

        append_column_names = list(source_to_target_columns_map.keys())
        target_column_names = list(source_to_target_columns_map.values())
        update_statements = [f"{col_name}=EXCLUDED.{col_name}" for col_name in target_column_names]

        query = statement.format(
            target_columns=",".join(target_column_names),
            main_table=target_table.name,
            append_columns=",".join(append_column_names),
            source_table=source_table.name,
            update_statements=",".join(update_statements),
            merge_keys=",".join(list(target_conflict_columns)),
        )
        self.run_sql(sql=query)

    def get_sqla_table(self, table: BaseTable) -> SqlaTable:
        """
        Return SQLAlchemy table instance

        :param table: Astro Table to be converted to SQLAlchemy table instance
        """
        return SqlaTable(table.name, SqlaMetaData(), autoload_with=self.sqlalchemy_engine)

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: /tmp/local.duckdb.table_name
        """
        conn = self.hook.get_connection(self.conn_id)
        return f"{conn.host}.{table.name}"

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: duckdb://127.0.0.1:22
        """
        conn = self.hook.get_connection(self.conn_id)
        port = conn.port or 22
        return f"file://{socket.gethostbyname(socket.gethostname())}:{port}"

    def openlineage_dataset_uri(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace()}{self.openlineage_dataset_name(table=table)}"
