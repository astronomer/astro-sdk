from typing import Optional, Union

from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.result import ResultProxy

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

    def run_sql(
        self, sql_statement: Union[text, str], parameters: Optional[dict] = None
    ) -> ResultProxy:
        """
        Run given SQL statement in the database using the Sqlalchemy engine.

        :param sql_statement: SQL statement to be run on the engine
        :param parameters: (optional) Parameters to be passed to the SQL statement
        :return: Result of running the statement.
        """
        if parameters is None:
            parameters = {}
        return self.connection.execute(sql_statement, parameters)

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
