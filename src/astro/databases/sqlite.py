from typing import List, Optional

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from astro.constants import AppendConflictStrategy
from astro.databases.base import BaseDatabase
from astro.sql.table import Metadata, Table

DEFAULT_CONN_ID = SqliteHook.default_conn_name


class SqliteDatabase(BaseDatabase):
    """
    Handle interactions with Sqlite databases. If this class is successful, we should not have any Sqlite-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self) -> SqliteHook:
        """Retrieve Airflow hook to interface with the Sqlite database."""
        return SqliteHook(sqlite_conn_id=self.conn_id)

    @property
    def sqlalchemy_engine(self) -> Engine:
        """Return SQAlchemy engine."""
        # Airflow uses sqlite3 library and not SqlAlchemy for SqliteHook
        # and it only uses the hostname directly.
        airflow_conn = self.hook.get_connection(self.conn_id)
        return create_engine(f"sqlite:///{airflow_conn.host}")

    @property
    def default_metadata(self) -> Metadata:
        """Since Sqlite does not use Metadata, we return an empty Metadata instances."""
        return Metadata()

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    @staticmethod
    def get_table_qualified_name(table: Table) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        return str(table.name)

    def populate_table_metadata(self, table: Table) -> Table:
        """
        Since SQLite does not have a concept of databases or schemas, we just return the table as is,
        without any modifications.
        """
        return table

    def create_schema_if_needed(self, schema: Optional[str]) -> None:
        """
        Since SQLite does not have schemas, we do not need to set a schema here.
        """

    def schema_exists(self, schema: str) -> bool:
        """
        Check if a schema exists. We return false for sqlite since sqlite does not have schemas
        """
        return False

    def _merge_table(
        self,
        source_table: Table,
        target_table: Table,
        if_conflicts: AppendConflictStrategy,
        target_conflict_columns: List[str],
        target_columns: List[str],
        source_columns: List[str],
    ):
        statement = "INSERT INTO {main_table} ({target_columns}) SELECT {append_columns} FROM {append_table} Where true"

        if if_conflicts == "ignore":
            statement += " ON CONFLICT ({merge_keys}) DO NOTHING"
        elif if_conflicts == "update":
            statement += " ON CONFLICT ({merge_keys}) DO UPDATE SET {update_statements}"

        append_column_names = list(source_columns)
        target_column_names = list(target_columns)
        column_pairs = list(zip(target_column_names, target_column_names))
        update_statements = [f"{x}=EXCLUDED.{y}" for x, y in column_pairs]

        query = statement.format(
            target_columns=",".join(target_column_names),
            main_table=target_table.name,
            append_columns=",".join(append_column_names),
            append_table=source_table.name,
            update_statements=",".join(update_statements),
            merge_keys=",".join(list(target_conflict_columns)),
        )
        return query
