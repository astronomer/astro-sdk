from typing import Dict

import pandas as pd
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import MetaData, create_engine
from sqlalchemy.engine.base import Engine
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    AppendConflictStrategy,
    LoadExistStrategy,
)
from astro.databases.base import BaseDatabase
from astro.sql.tables import Table

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

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    def get_table_qualified_name(self, table: Table) -> str:
        """
        Return the table qualified name.

        :param table: The table we want to retrieve the qualified name for.
        """
        return str(table.name)

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

    def get_sqla_table_object(self, table: Table) -> SqlaTable:
        """
        Return SQLAlchemy table object using reflections

        :param table: The table we want to retrieve the qualified name for.
        """
        metadata_params: Dict[str, str] = {}
        metadata = MetaData(**metadata_params)
        return SqlaTable(
            self.get_table_qualified_name(table),
            metadata,
            autoload_with=self.sqlalchemy_engine,
        )

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
        source_dataframe.to_sql(
            target_table.name,
            con=self.sqlalchemy_engine,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )
