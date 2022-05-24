"""Postgres database implementation."""
from typing import Dict, List, Optional

import pandas as pd
import sqlalchemy
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql as postgres_sql


from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.settings import SCHEMA

from astro.constants import AppendConflictStrategy

from astro.databases.base import BaseDatabase

from astro.sql.table import Metadata, Table

DEFAULT_CONN_ID = PostgresHook.default_conn_name


class PostgresDatabase(BaseDatabase):
    """
    Handle interactions with Postgres databases. If this class is successful, we should not have any Postgres-specific
    logic in other parts of our code-base.
    """

    illegal_column_name_chars: List[str] = ["."]
    illegal_column_name_chars_replacement: List[str] = ["_"]

    _combine_table_statement: str = (
        "INSERT INTO {main_table} ({target_columns})"
        " SELECT {append_columns} FROM {append_table}"
    )
    _combine_table_conflict_ignore_statement: str = (
        " ON CONFLICT ({merge_keys}) DO NOTHING"
    )
    _combine_table_conflict_update_statement: str = (
        " ON CONFLICT ({merge_keys}) DO UPDATE SET {update_statements}"
    )

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

    def combine_tables(
        self,
        source_table: Table,
        target_table: Table,
        source_to_target_columns_map: Dict[str, str],
        target_conflict_columns: Optional[List[str]] = None,
        conflict_strategy: AppendConflictStrategy = "exception",
    ) -> None:
        """Combine two tables based on target_conflict_columns

        :param source_table: Contains the table to be added to the target_table
        :param target_table: Contains the destination table in which the rows will be added
        :param target_conflict_columns: List of cols where we expect to have a conflict while combining
        :param conflict_strategy: Action that needs to be taken in case there is a
        conflict due to col constraint
        :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names,
        """
        target_conflict_columns = target_conflict_columns or []
        source_to_target_columns_map = source_to_target_columns_map or {}

        source_tables_cols: List[str] = list(source_to_target_columns_map.keys())
        target_tables_cols: List[str] = list(source_to_target_columns_map.values())

        def identifier_args(table):
            schema = table.metadata.schema
            return (schema, table.name) if schema else (table.name,)

        if conflict_strategy != "exception" and len(target_conflict_columns) > 0:
            if conflict_strategy == "ignore":
                self._combine_table_statement += (
                    self._combine_table_conflict_ignore_statement
                )
            elif conflict_strategy == "update":
                self._combine_table_statement += (
                    self._combine_table_conflict_update_statement
                )

        source_tables_cols = [postgres_sql.Identifier(c) for c in source_tables_cols]
        target_tables_cols = [postgres_sql.Identifier(c) for c in target_tables_cols]
        column_pairs = list(zip(source_tables_cols, target_tables_cols))
        update_statements = [
            postgres_sql.SQL("{x}=EXCLUDED.{y}").format(x=x, y=y)
            for x, y in column_pairs
        ]

        query = postgres_sql.SQL(self._combine_table_statement).format(
            target_columns=postgres_sql.SQL(",").join(target_tables_cols),
            main_table=postgres_sql.Identifier(*identifier_args(target_table)),
            append_columns=postgres_sql.SQL(",").join(source_tables_cols),
            append_table=postgres_sql.Identifier(*identifier_args(source_table)),
            update_statements=postgres_sql.SQL(",").join(update_statements),
            merge_keys=postgres_sql.SQL(",").join(
                [postgres_sql.Identifier(x) for x in target_conflict_columns]
            ),
        )

        sql = query.as_string(self.hook.get_conn())
        self.run_sql(sql_statement=sql)
