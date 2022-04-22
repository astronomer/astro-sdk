import pathlib
from typing import Optional, Union

import pandas as pd
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.result import ResultProxy

from astro.constants import LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.sql.tables import Table


class Database(BaseDatabase):
    @property
    def hook(self):
        """
        Retrieve Airflow hook to interface with the Sqlite database.
        """
        return SqliteHook(sqlite_conn_id=self.conn_id)

    @property
    def sqlalchemy_engine(self) -> Engine:
        """
        Return SQAlchemy engine.
        """
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

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    def get_table_qualified_name(self, table: Table) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        return table.name

    # ---------------------------------------------------------
    # Load methods
    # ---------------------------------------------------------
    def load_file_to_table(
        self,
        source_file: Union[str, pathlib.Path],
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
    ) -> None:
        """
        Upload the content of the source file to the target database.
        If the table instance does not contain columns, this method automatically identify them using Pandas.

        :param source_file: Path to original file (e.g. a "/tmp/sample_data.csv")
        :param target_table: Details of the target table
        :param if_exists: Strategy to be applied in case the target table exists
        """
        raise NotImplementedError

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
    ) -> None:
        """
        Create a table with the dataframe's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_dataframe: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        """
        raise NotImplementedError

    def append_table(
        self,
        source_table: Table,
        target_table: Table,
        if_conflicts: MergeConflictStrategy = "exception",
    ):
        """
        Append the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """
        raise NotImplementedError

    # ---------------------------------------------------------
    # Extract methods
    # ---------------------------------------------------------
    def export_table_to_file(
        self, source_table: Table, target_file: Union[str, pathlib.Path]
    ) -> None:
        """
        Copy the content of a table to a target file of supported type, in a supported location.

        :param source_table: An existing table in the database
        :param target_file: The path to the file to which we aim to dump the content of the database.
        """
        # TODO: we probably want to have a class to abstract File. This would allow us to not have to rely on the file
        # extension to decide what is the file format.
        raise NotImplementedError

    def export_table_to_pandas_dataframe(self, source_table: Table) -> pd.DataFrame:
        """
        Copy the content of a table to an in-memory Pandas dataframe.

        :param source_table: An existing table in the database"""
        raise NotImplementedError
