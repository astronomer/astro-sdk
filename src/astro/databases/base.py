import pathlib
from abc import ABCMeta
from typing import List, Optional, Union

import pandas as pd
import sqlalchemy
from airflow.hooks.base import BaseHook

from astro.constants import LoadExistStrategy, MergeConflictStrategy
from astro.sql.tables import Table


class Database(metaclass=ABCMeta):
    """
    Base class to represent all the Database interactions.

    The goal is to be able to support new databases by adding
    a new module to the `astro/databases` directory, without the need of
    changing other modules and classes.

    The exception is if the Airflow connection type does not match the new
    Database module name. In that case, we should update the dictionary
    `CUSTOM_CONN_TYPE_TO_MODULE_PATH` available at `astro/databases/__init__.py`.
    """

    _create_schema_statement: str = "CREATE SCHEMA IF NOT EXISTS {}"
    _drop_table_statement: str = "DROP TABLE IF EXISTS {}"
    _create_table_statement: str = ""

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    @property
    def hook(self) -> BaseHook:
        """
        Return an instance of the database-specific Airflow hook.
        """
        raise NotImplementedError

    @property
    def connection(self) -> sqlalchemy.engine.base.Connection:
        """
        Return a Sqlalchemy connection object for the given database.
        """
        return self.sqlalchemy_engine.connect()

    @property
    def sqlalchemy_engine(self) -> sqlalchemy.engine.base.Engine:
        """
        Return Sqlalchemy engine.
        """
        return self.hook.get_sqlalchemy_engine()

    def run_sql(self, sql_statement: str, parameters: Optional[dict] = None):
        """
        Return the results to running a SQL statement.

        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql_statement: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        """
        return self.hook.run(sql_statement, parameters)

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    def get_table_qualified_name(self, table: Table) -> str:
        """
        Return table qualified name. This is Database-specific.
        For instance, in Sqlite this is the table name. In Snowflake, however, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        # Initially this method belonged to the Table class.
        # However, in order to have an agnostic table class implementation,
        # we are keeping all methods which vary depending on the database within the Database class.
        raise NotImplementedError

    # ---------------------------------------------------------
    # Table creation & deletion methods
    # ---------------------------------------------------------
    def create_table(self, table: Table) -> None:
        """
        Create a SQL table. For this method to work, the table instance must contain columns.

        :param table: The table to be created.
        """
        if table.metadata:
            metadata = sqlalchemy.MetaData(schema=table.metadata.schema)
            sqlalchemy_table = sqlalchemy.Table(table.name, metadata, *table.columns)
        else:
            sqlalchemy_table = sqlalchemy.Table(table.name, None, *table.columns)
        metadata.create_all(
            self.sqlalchemy_engine, tables=[sqlalchemy_table]
        )  # mypy: ignore-errors

    def create_table_from_statement(
        self,
        statement: str,
        target_table: Table,
        source_tables: Optional[List[Table]] = None,
        parameters: Optional[dict] = None,
    ) -> None:
        """
        Export the result rows of a query statement into another table.

        :param statement: SQL query statement
        :param target_table: Destination table where results will be recorded.
        :param parameters: (Optional) parameters to be used to render the SQL query
        :param source_tables: (Optional) list of tables used by the SQL statement
        """
        raise NotImplementedError

    def drop_table(self, table: Table) -> None:
        """
        Delete a SQL table, if it exists.

        :param table: The table to be deleted.
        """
        statement = self._drop_table_statement.format(
            self.get_table_qualified_name(table)
        )
        self.hook.run(statement)

    # ---------------------------------------------------------
    # Table load methods
    # ---------------------------------------------------------
    def load_file_to_table(
        self,
        source_file: Union[str, pathlib.Path],
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
    ) -> None:
        """
        Create a table with the file's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_file: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
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

    def export_table_to_dataframe(self, source_table: Table) -> pd.DataFrame:
        """
        Copy the content of a table to an in-memory Pandas dataframe.

        :param source_table: An existing table in the database"""
        raise NotImplementedError

    # ---------------------------------------------------------
    # Transformation methods
    # ---------------------------------------------------------

    def append_tables(
        self,
        source_tables: List[Table],
        target_table: Table,
        if_conflicts: MergeConflictStrategy = "exception",
    ):
        """
        Append the source tables into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_tables: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """
        raise NotImplementedError
