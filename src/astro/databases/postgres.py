# from typing import Optional, Union

from airflow.providers.postgres.hooks.postgres import PostgresHook

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    AppendConflictStrategy,
    LoadExistStrategy,
)
from astro.databases.base import BaseDatabase
from astro.sql.tables import Table
from astro.utils.load import load_file_into_dataframe

# from sqlalchemy import create_engine, text
# from sqlalchemy.engine.base import Engine
# from sqlalchemy.engine.result import ResultProxy


DEFAULT_CONN_ID = PostgresHook.default_conn_name


class PostgresDatabase(BaseDatabase):
    """
    Handle interactions with Postgres databases. If this class is successful, we should not have any Postgres-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self):
        """Retrieve Airflow hook to interface with the Postgres database."""
        return PostgresHook(postgres_conn_id=self.conn_id)

    # ---------------------------------------------------------
    # Load methods
    # ---------------------------------------------------------
    def load_file_to_table(
        self,
        source_file: str,  # TODO: replace by File object, which will contain normalization config
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Upload the content of the source file to the target database.
        If the table instance does not contain columns, this method automatically identify them using Pandas.

        :param source_file: Path to original file (e.g. a "/tmp/sample_data.csv")
        :param target_table: Details of the target table
        :param if_exists: Strategy to be applied in case the target table exists
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        pandas_dataframe = load_file_into_dataframe(source_file)
        self.load_pandas_dataframe_to_table(
            pandas_dataframe, target_table, if_exists, chunk_size
        )

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
