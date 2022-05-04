from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from astro.databases.base import BaseDatabase
from astro.sql.tables import Metadata, Table

DEFAULT_CONN_ID = SqliteHook.default_conn_name


class SqliteDatabase(BaseDatabase):
    """
    Handle interactions with Sqlite databases. If this class is successful, we should not have any Sqlite-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self):
        """Retrieve Airflow hook to interface with the Sqlite database."""
        return SqliteHook(sqlite_conn_id=self.conn_id)

    @property
    def sqlalchemy_engine(self) -> Engine:
        """Return SQAlchemy engine."""
        uri = self.hook.get_uri()
        if "////" not in uri:
            uri = uri.replace("///", "////")
        return create_engine(uri)

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

    def set_schema_if_needed(self, schema):
        """
        Since SQLite does not have schemas, we do not need to set a schema here
        :param schema:
        :return:
        """
        pass

    def schema_exists(self, schema):
        """

        :param schema:
        :return:
        """
        return False
