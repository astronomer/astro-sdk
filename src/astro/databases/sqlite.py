from typing import Optional, Union

import pandas as pd
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.result import ResultProxy

from astro.databases.base import Database as BaseDatabase
from astro.sql.tables import Table


class Database(BaseDatabase):
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
        connection = self.sqlalchemy_engine.connect()
        return connection.execute(sql_statement, parameters)

    # ---------------------------------------------------------
    # Load methods
    # ---------------------------------------------------------
    def load_file_to_table(self, source_file: str, target_table: Table) -> None:
        raise NotImplementedError

    def load_pandas_dataframe_to_table(
        self, source_dataframe: pd.DataFrame, target_table: Table
    ) -> None:
        raise NotImplementedError

    # ---------------------------------------------------------
    # Extract methods
    # ---------------------------------------------------------
    def export_table_to_file(self, source_table: Table, target_file: str) -> None:
        raise NotImplementedError

    def export_table_to_pandas_dataframe(self, source_table: Table) -> pd.DataFrame:
        raise NotImplementedError

    # ---------------------------------------------------------
    # Transformation methods
    # ---------------------------------------------------------
    def select_from_table_to_table(
        self, source_table: Table, target_table: Table
    ) -> None:
        raise NotImplementedError

    def merge_tables(self, source_table: Table, target_table: Table) -> None:
        raise NotImplementedError
