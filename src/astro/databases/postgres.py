"""Postgres database implementation."""
from typing import List

import pandas as pd
import sqlalchemy
from airflow.providers.postgres.hooks.postgres import PostgresHook

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.databases.base import BaseDatabase
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table

DEFAULT_CONN_ID = PostgresHook.default_conn_name


class PostgresDatabase(BaseDatabase):
    """
    Handle interactions with Postgres databases. If this class is successful, we should not have any Postgres-specific
    logic in other parts of our code-base.
    """

    illegal_column_name_chars: List[str] = ["."]
    illegal_column_name_chars_replacement: List[str] = ["_"]

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self):
        """Retrieve Airflow hook to interface with the Postgres database."""
        return PostgresHook(postgres_conn_id=self.conn_id)

    @property
    def default_metadata(self) -> Metadata:
        """Fill in default metadata values for table objects addressing postgres databases"""
        database = self.hook.get_connection(self.conn_id).schema
        return Metadata(database=database, schema=SCHEMA)

    def schema_exists(self, schema) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        schema_result = self.hook.run(
            "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = lower(%(schema_name)s);",
            parameters={"schema_name": schema.lower()},
            handler=lambda x: [y[0] for y in x.fetchall()],
        )
        return len(schema_result) > 0

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
        schema = None
        if target_table.metadata and target_table.metadata.schema:
            self.create_schema_if_needed(target_table.metadata.schema)
            schema = target_table.metadata.schema.lower()
        source_dataframe.to_sql(
            target_table.name,
            schema=schema,  # type: ignore
            con=self.sqlalchemy_engine,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )

    @staticmethod
    def get_table_qualified_name(table: Table) -> str:  # skipcq: PYL-R0201
        """
        Return table qualified name. This is Database-specific.
        For instance, in Sqlite this is the table name. In Snowflake, however, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        # Initially this method belonged to the Table class.
        # However, in order to have an agnostic table class implementation,
        # we are keeping all methods which vary depending on the database within the Database class.
        if table.metadata and table.metadata.schema:
            qualified_name = f"{table.metadata.schema.lower()}.{table.name}"  # type: ignore
        else:
            qualified_name = table.name
        return qualified_name

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the database

        :param table: Details of the table we want to check that exists
        """
        _schema = table.metadata.schema
        # when creating schemas they are created in a lowercase even when we have a schema in uppercase.
        # while checking for schema there in no lowercase applied in 'has_table()' which leads to table not found.
        # Added 'schema.lower()' to make sure we search for schema in lowercase to match the creation lowercase.
        schema = _schema.lower() if _schema else _schema

        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(inspector.dialect.has_table(self.connection, table.name, schema))
