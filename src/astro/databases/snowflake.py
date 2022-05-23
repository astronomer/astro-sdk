"""Snowflake database implementation."""
from typing import List, Optional, Tuple

import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pandas.io.sql import SQLDatabase

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    DBMergeConflictStrategy,
    LoadExistStrategy,
)
from astro.databases.base import BaseDatabase
from astro.sql.table import Metadata, Table
from astro.utils.dependencies import pandas_tools
from astro.utils.snowflake_merge_func import snowflake_merge_func

DEFAULT_CONN_ID = SnowflakeHook.default_conn_name


class SnowflakeDatabase(BaseDatabase):
    """
    Handle interactions with snowflake databases. If this class is successful, we should not have any snowflake-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self):
        """Retrieve Airflow hook to interface with the snowflake database."""
        return SnowflakeHook(snowflake_conn_id=self.conn_id)

    @property
    def default_metadata(self) -> Metadata:
        """
        Fill in default metadata values for table objects addressing snowflake databases
        """
        connection = self.hook.get_conn()
        return Metadata(
            schema=connection.schema,
            database=connection.database,
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
        db = SQLDatabase(engine=self.sqlalchemy_engine)
        # Make columns uppercase to prevent weird errors in snowflake
        source_dataframe.columns = source_dataframe.columns.str.upper()
        schema = None
        if target_table.metadata:
            schema = getattr(target_table.metadata, "schema", None)

        # within prep_table() we use pandas drop() function which is used when we pass 'if_exists=replace'.
        # There is an issue where has_table() works with uppercase table names but the function meta.reflect() don't.
        # To prevent the issue we are passing table name in lowercase.
        db.prep_table(
            source_dataframe,
            target_table.name.lower(),
            schema=schema,
            if_exists=if_exists,
            index=False,
        )
        pandas_tools.write_pandas(
            self.hook.get_conn(),
            source_dataframe,
            target_table.name,
            chunk_size=chunk_size,
            quote_identifiers=False,
        )

    def get_sqlalchemy_template_table_identifier_and_parameter(
        self, table: Table, jinja_table_identifier: str
    ) -> Tuple[str, str]:
        """
        During the conversion from a Jinja-templated SQL query to a SQLAlchemy query, there is the need to
        convert a Jinja table identifier to a safe SQLAlchemy-compatible table identifier.

        For Snowflake, the query:
            sql_statement = "SELECT * FROM {{input_table}};"
            parameters = {"input_table": Table(name="user_defined_table", metadata=Metadata(schema="some_schema"))}

        Will become
            "SELECT * FROM IDENTIFIER(:input_table);"
            parameters = {"input_table": "some_schema.user_defined_table"}

        Example of usage:
            jinja_table_identifier, jinja_table_parameter_value = \
                get_sqlalchemy_template_table_identifier_and_parameter(
                    Table(name="user_defined_table", metadata=Metadata(schema="some_schema"),
                    "input_table"
                )
            assert jinja_table_identifier == "IDENTIFIER(:input_table)"
            assert jinja_table_parameter_value == "some_schema.user_defined_table"

        Since the table value is templated, there is a safety concern (e.g. SQL injection).
        We recommend looking into the documentation of the database and seeing what are the best practices.
        This is the Snowflake documentation:
        https://docs.snowflake.com/en/sql-reference/identifier-literal.html

        :param table: The table object we want to generate a safe table identifier for
        :param jinja_table_identifier: The name used within the Jinja template to represent this table
        :return: value to replace the table identifier in the query and the value that should be used to replace it
        """
        return (
            f"IDENTIFIER(:{jinja_table_identifier})",
            SnowflakeDatabase.get_table_qualified_name(table),
        )

    def schema_exists(self, schema) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        created_schemas = [
            x["SCHEMA_NAME"]
            for x in self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
            )
        ]
        return len(created_schemas) == 1

    def append_table(
        self,
        source_table: Table,
        target_table: Table,
        source_tables_cols: Optional[List[str]] = None,
        target_tables_cols: Optional[List[str]] = None,
    ) -> None:
        """
        Append the source table rows into a destination table
        The argument `conflict_strategy` allows the user to define how to handle conflicts

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param conflict_strategy: Action that needs to be taken in case there is a conflict
        :param source_tables_cols: List of columns name in source table that will be used in appending
        :param target_tables_cols: List of columns name in target table that will be used in appending
        """

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
        self.run_sql(
            snowflake_merge_func(
                merge_table=source_table,
                target_table=target_table,
                target_columns=target_tables_cols,
                merge_columns=source_tables_cols,
                merge_keys=merge_cols,
                conflict_strategy=conflict_strategy,
            )
        )
