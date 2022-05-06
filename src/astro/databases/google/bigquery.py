from typing import Optional, Union

import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from sqlalchemy import create_engine, text
from sqlalchemy.engine.base import Engine
from sqlalchemy.engine.result import ResultProxy

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    AppendConflictStrategy,
    LoadExistStrategy,
)
from astro.databases.base import BaseDatabase
from astro.sql.tables import Table

DEFAULT_CONN_ID = BigQueryHook.default_conn_name


class BigqueryDatabase(BaseDatabase):
    """
    Handle interactions with Bigquery databases. If this class is successful, we should not have any Bigquery-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self):
        """Retrieve Airflow hook to interface with the Sqlite database."""
        return BigQueryHook(gcp_conn_id=self.conn_id)

    @property
    def sqlalchemy_engine(self) -> Engine:
        """Return SQAlchemy engine."""
        uri = self.hook.get_uri()
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

    def get_table_qualified_name(self, table: Table) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        qualified_name: str = (
            table.metadata.schema + "." + table.name
            if table.metadata.schema
            else table.name
        )
        return qualified_name

    def append_table(
        self,
        source_table: Table,
        target_table: Table,
        if_conflicts: AppendConflictStrategy = "exception",
    ):
        """
        Append the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param if_conflicts: The strategy to be applied if there are conflicts. Options:
            * exception: Raises an exception if there is a conflict
            * ignore: Ignores the source row value if it conflicts with a value in the target table
            * update: Updates the target row with the content of the source file
        """
        # TODO: implement this method.
        # previous append implementation
        # -> raises exception
        # previous merge implementation
        # -> ignore / update
        # select(target_table.columns).from_select(source_table.columns)

        raise NotImplementedError

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Create a table with the dataframe's contents.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_dataframe: Local or remote filepath
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists.
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        source_dataframe.to_gbq(
            self.get_table_qualified_name(target_table),
            if_exists=if_exists,
            chunksize=chunk_size,
            project_id=self.hook.project_id,
        )
