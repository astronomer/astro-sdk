"""Snowflake database implementation."""
from typing import List, Tuple

import pandas as pd
from airflow.exceptions import AirflowException
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pandas.io.sql import SQLDatabase

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    AppendConflictStrategy,
    LoadExistStrategy,
)
from astro.databases.base import BaseDatabase
from astro.sql.table import Metadata, Table
from astro.utils.dependencies import pandas_tools

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

    def _wrap_identifier(self, inp):
        return "Identifier({{" + inp + "}})"

    def _merge_table(
        self,
        source_table: Table,
        target_table: Table,
        if_conflicts: AppendConflictStrategy,
        target_conflict_columns: List[str],
        target_columns: List[str],
        source_columns: List[str],
    ):
        statement = (
            "merge into {{main_table}} using {{merge_table}} on " "{merge_clauses}"
        )

        if isinstance(target_conflict_columns, dict):
            merge_keys_keys = target_conflict_columns.keys()
            merge_keys_values = target_conflict_columns.values()
        else:
            merge_keys_keys = merge_keys_values = target_conflict_columns

        merge_target_dict = {
            "merge_clause_target_" + str(i): target_table.name + "." + x
            for i, x in enumerate(merge_keys_keys)
        }
        merge_append_dict = {
            "merge_clause_append_" + str(i): source_table.name + "." + x
            for i, x in enumerate(merge_keys_values)
        }

        statement = self._fill_in_merge_clauses(
            merge_append_dict, merge_target_dict, statement
        )

        values_to_check = [target_table.name, source_table.name]
        values_to_check.extend(target_columns)
        values_to_check.extend(source_columns)
        for v in values_to_check:
            if not self._is_valid_snow_identifier(v):
                raise AirflowException(
                    f"The identifier {v} is invalid. Please check to prevent SQL injection"
                )

        if if_conflicts == "update":
            statement += " when matched then UPDATE SET {merge_vals}"
            statement = self.fill_in_update_statement(
                statement,
                target_table.name,
                source_table.name,
                target_columns,
                source_columns,
            )
        statement += (
            " when not matched then insert({target_columns}) values ({append_columns})"
        )
        statement = self.fill_in_append_statements(
            target_table.name,
            source_table.name,
            statement,
            target_columns,
            source_columns,
        )

        params = {}
        params.update(merge_target_dict)
        params.update(merge_append_dict)
        params["main_table"] = target_table
        params["merge_table"] = source_table

        return statement, params

    def fill_in_append_statements(
        self,
        main_table,
        merge_table,
        statement,
        targest_columns,
        merge_columns,
    ):
        statement = statement.replace(
            "{target_columns}",
            ",".join(f"{main_table}.{t}" for t in targest_columns),
        )
        statement = statement.replace(
            "{append_columns}",
            ",".join(f"{merge_table}.{t}" for t in merge_columns),
        )
        return statement

    def fill_in_update_statement(
        self, statement, main_table, merge_table, target_columns, merge_columns
    ):
        merge_statement = ",".join(
            [
                f"{main_table}.{t}={merge_table}.{m}"
                for t, m in zip(target_columns, merge_columns)
            ]
        )
        statement = statement.replace("{merge_vals}", merge_statement)
        return statement

    def _fill_in_merge_clauses(self, merge_append_dict, merge_target_dict, statement):
        return statement.replace(
            "{merge_clauses}",
            " AND ".join(
                self._wrap_identifier(k) + "=" + self._wrap_identifier(v)
                for k, v in zip(merge_target_dict.keys(), merge_append_dict.keys())
            ),
        )

    def _is_valid_snow_identifier(self, name):
        """
        Because Snowflake does not allow using `Identifier` for inserts or updates, we need to make reasonable
        attempts to ensure that no one can perform a SQL injection using this method. The following method ensures
        that a string follows the expected identifier syntax
        https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
        @param name:
        @return:
        """
        if not 1 <= len(name) <= 255:
            return False

        name_is_quoted = name[0] == '"'
        if name_is_quoted:
            if len(name) < 2 or name[-1] != '"':
                return False  # invalid because no closing quote

            return self._ensure_internal_quotes_closed(name)
        return self._ensure_only_valid_characters(name)

    # test code to check for validate snowflake identifier
    def _ensure_internal_quotes_closed(self, name):
        last_quoted = False
        for c in name[1:-1]:
            if last_quoted:
                if c != '"':
                    return False
                last_quoted = False
            elif c == '"':
                last_quoted = True
            # any character is fair game inside a properly quoted name

        if last_quoted:
            return False  # last quote was not escape

        return True

    def _ensure_only_valid_characters(self, name):
        if not (name[0].isalpha()) and name[0] != "_":
            return False
        for c in name[1:]:
            if not (c.isalpha() or c.isdigit() or c == "_" or c == "$"):
                return False
        return True
