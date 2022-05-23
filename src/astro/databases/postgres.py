"""Postgres database implementation."""
from typing import List, Optional

import pandas as pd
import sqlalchemy
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql as postgres_sql

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    DBMergeConflictStrategy,
    LoadExistStrategy,
)
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

    def merge_table(
        self,
        source_table: Table,
        target_table: Table,
        conflict_strategy: DBMergeConflictStrategy = "ignore",
        merge_cols: Optional[List[str]] = None,
        source_tables_cols: Optional[List[str]] = None,
        target_tables_cols: Optional[List[str]] = None,
    ) -> None:
        """Merge two tables based on merge keys

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param conflict_strategy: Action that needs to be taken in case there is a conflict
        :param merge_cols: List of cols that are checked for uniqueness while merging,
         they will be the unique post merge
        :param source_tables_cols: List of columns name in source table that will be used in merging
        :param target_tables_cols: List of columns name in target table that will be used in merging
        """
        source_tables_cols = source_tables_cols or []
        target_tables_cols = target_tables_cols or []
        merge_cols = merge_cols or []

        if len(source_tables_cols) != len(target_tables_cols):
            raise ValueError(
                "Params 'source_tables_cols' and  'target_tables_cols' should contain same number of cols."
            )

        def identifier_args(table):
            schema = table.metadata.schema
            return (schema, table.name) if schema else (table.name,)

        statement = "INSERT INTO {main_table} ({target_columns}) SELECT {append_columns} FROM {append_table}"

        if len(merge_cols) > 0:
            if conflict_strategy == "ignore":
                statement += " ON CONFLICT ({merge_keys}) DO NOTHING"
            elif conflict_strategy == "update":
                statement += (
                    " ON CONFLICT ({merge_keys}) DO UPDATE SET {update_statements}"
                )

        source_tables_cols = [postgres_sql.Identifier(c) for c in source_tables_cols]
        target_tables_cols = [postgres_sql.Identifier(c) for c in target_tables_cols]
        column_pairs = list(zip(target_tables_cols, target_tables_cols))
        update_statements = [
            postgres_sql.SQL("{x}=EXCLUDED.{y}").format(x=x, y=y)
            for x, y in column_pairs
        ]

        query = postgres_sql.SQL(statement).format(
            target_columns=postgres_sql.SQL(",").join(target_tables_cols),
            main_table=postgres_sql.Identifier(*identifier_args(target_table)),
            append_columns=postgres_sql.SQL(",").join(source_tables_cols),
            append_table=postgres_sql.Identifier(*identifier_args(source_table)),
            update_statements=postgres_sql.SQL(",").join(update_statements),
            merge_keys=postgres_sql.SQL(",").join(
                [postgres_sql.Identifier(x) for x in merge_cols]
            ),
        )

        hook = PostgresHook(postgres_conn_id=target_table.conn_id)
        sql = query.as_string(hook.get_conn())
        self.run_sql(sql_statement=sql)
