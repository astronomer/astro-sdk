from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

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

    def get_sqlalchemy_engine(self, table: Table):
        # Airflow uses sqlite3 library and not SqlAlchemy for SqliteHook
        # and it only uses the hostname directly.
        airflow_conn = self.hook.get_connection(self.conn_id)
        return create_engine(f"sqlite:///{airflow_conn.host}")

    @property
    def default_metadata(self) -> Metadata:
        return Metadata()

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    def get_table_qualified_name(self, table: Table) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        return str(table.name)

    def populate_table_metadata(self, table: Table):
        """
        Since SQLite does not have a concept of tables or schemas, we just return the table as is
        :param table:
        :return:
        """
        return table

    def create_schema_if_needed(self, schema: str) -> None:
        """
        Since SQLite does not have schemas, we do not need to set a schema here
        :param schema:
        :return:
        """

    def schema_exists(self, schema: str) -> bool:
        """
        Check if a schema exists. We return false for sqlite since sqlite does not have schemas

        :param schema:
        :return:
        """
        return False
