from abc import ABC
from typing import Dict, List, Optional, Tuple, Union

import pandas as pd
import sqlalchemy
from airflow.hooks.dbapi import DbApiHook
from sqlalchemy import column, insert, select
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    ExportExistsStrategy,
    LoadExistStrategy,
    MergeConflictStrategy,
)
from astro.exceptions import NonExistentTableException
from astro.files import File
from astro.settings import SCHEMA
from astro.sql.table import Metadata, Table


class BaseDatabase(ABC):
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
    _create_table_statement: str = "CREATE TABLE IF NOT EXISTS {} AS {}"

    # Used to normalize the ndjson when appending fields in nested fields.
    # Example -
    #   ndjson - {'a': {'b': 'val'}}
    #   the col names generated is 'a.b'. char '.' maybe an illegal char in some db's col name.
    # Contains the illegal char and there replacement, where the value in
    # illegal_column_name_chars[0] will be replaced by value in illegal_column_name_chars_replacement[0]
    illegal_column_name_chars: List[str] = []
    illegal_column_name_chars_replacement: List[str] = []

    def __init__(self, conn_id: str):
        self.conn_id = conn_id

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.conn_id})'

    @property
    def sql_type(self):
        raise NotImplementedError

    @property
    def hook(self) -> DbApiHook:
        """Return an instance of the database-specific Airflow hook."""
        raise NotImplementedError

    @property
    def connection(self) -> sqlalchemy.engine.base.Connection:
        """Return a Sqlalchemy connection object for the given database."""
        return self.sqlalchemy_engine.connect()

    @property
    def sqlalchemy_engine(self) -> sqlalchemy.engine.base.Engine:
        """Return Sqlalchemy engine."""
        return self.hook.get_sqlalchemy_engine()  # type: ignore[no-any-return]

    def run_sql(
        self,
        sql_statement: Union[str, ClauseElement],
        parameters: Optional[dict] = None,
    ):
        """
        Return the results to running a SQL statement.

        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql_statement: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        """
        if parameters is None:
            parameters = {}

        if isinstance(sql_statement, str):
            result = self.connection.execute(sqlalchemy.text(sql_statement), parameters)
        else:
            result = self.connection.execute(sql_statement, parameters)
        return result

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        table_qualified_name = self.get_table_qualified_name(table)
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(inspector.dialect.has_table(self.connection, table_qualified_name))

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
    @staticmethod
    def get_merge_initialization_query(parameters: Tuple) -> str:
        """
        Handles database-specific logic to handle constraints, keeping
        it agnostic to database.
        """
        constraints = ",".join(parameters)
        sql = "ALTER TABLE {{table}} ADD CONSTRAINT airflow UNIQUE (%s)" % constraints
        return sql

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
            qualified_name = f"{table.metadata.schema}.{table.name}"
        else:
            qualified_name = table.name
        return qualified_name

    @property
    def default_metadata(self) -> Metadata:
        """
        Extract the metadata available within the Airflow connection associated with
        self.conn_id.

        :return: a Metadata instance
        """
        raise NotImplementedError

    def populate_table_metadata(self, table: Table) -> Table:
        """
        Given a table, check if the table has metadata.
        If the metadata is missing, and the database has metadata, assign it to the table.
        If the table schema was not defined by the end, retrieve the user-defined schema.
        This method performs the changes in-place and also returns the table.

        :param table: Table to potentially have their metadata changed
        :return table: Return the modified table
        """
        if table.metadata and table.metadata.is_empty() and self.default_metadata:
            table.metadata = self.default_metadata
        if not table.metadata.schema:
            table.metadata.schema = SCHEMA
        return table

    # ---------------------------------------------------------
    # Table creation & deletion methods
    # ---------------------------------------------------------
    def create_table(self, table: Table) -> None:
        """
        Create a SQL table. For this method to work, the table instance must contain columns.

        :param table: The table to be created.
        """
        metadata = table.sqlalchemy_metadata
        sqlalchemy_table = sqlalchemy.Table(table.name, metadata, *table.columns)
        metadata.create_all(self.sqlalchemy_engine, tables=[sqlalchemy_table])

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: Table,
        parameters: Optional[dict] = None,
    ) -> None:
        """
        Export the result rows of a query statement into another table.

        :param statement: SQL query statement
        :param target_table: Destination table where results will be recorded.
        :param parameters: (Optional) parameters to be used to render the SQL query
        """
        statement = self._create_table_statement.format(
            self.get_table_qualified_name(target_table), statement
        )
        self.run_sql(statement, parameters)

    def drop_table(self, table: Table) -> None:
        """
        Delete a SQL table, if it exists.

        :param table: The table to be deleted.
        """
        statement = self._drop_table_statement.format(
            self.get_table_qualified_name(table)
        )
        self.run_sql(statement)

    # ---------------------------------------------------------
    # Table load methods
    # ---------------------------------------------------------
    def load_file_to_table(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        """
        Load the content of the source file to the target database table.
        If the table already exists, append or replace the content, depending on the value of `if_exists`.

        :param source_file: Local or remote filepath (e.g. a File("/tmp/sample_data.csv"))
        :param target_table: Table in which the file will be loaded
        :param if_exists: Strategy to be used in case the target table already exists
        :param chunk_size: Specify the number of rows in each batch to be written at a time.
        """
        dataframe = source_file.export_to_dataframe()
        self.load_pandas_dataframe_to_table(
            dataframe,
            target_table,
            if_exists,
            chunk_size,
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
        self.create_schema_if_needed(target_table.metadata.schema)
        source_dataframe.to_sql(
            self.get_table_qualified_name(target_table),
            con=self.sqlalchemy_engine,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )

    def append_table(
        self,
        source_table: Table,
        target_table: Table,
        source_to_target_columns_map: Dict[str, str],
    ) -> None:
        """
        Append the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param source_to_target_columns_map: Dict of source_table columns names to target_table columns names
        """
        target_table_sqla = self.get_sqla_table(target_table)
        source_table_sqla = self.get_sqla_table(source_table)

        target_columns: List[ColumnClause]
        source_columns: List[ColumnClause]

        if not source_to_target_columns_map:
            target_columns = [column(col) for col in target_table_sqla.c.keys()]
            source_columns = target_columns
        else:
            target_columns = [
                column(col) for col in source_to_target_columns_map.values()
            ]
            source_columns = [
                column(col) for col in source_to_target_columns_map.keys()
            ]

        sel = select(source_columns).select_from(source_table_sqla)
        # TODO: We should fix the following Type Error
        # incompatible type List[ColumnClause[<nothing>]]; expected List[Column[Any]]
        sql = insert(target_table_sqla).from_select(target_columns, sel)  # type: ignore[arg-type]
        self.run_sql(sql_statement=sql)

    def merge_table(
        self,
        source_table: Table,
        target_table: Table,
        source_to_target_columns_map: Dict[str, str],
        target_conflict_columns: List[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ) -> None:
        """
        Merge the source table rows into a destination table.
        The argument `if_conflicts` allows the user to define how to handle conflicts.

        :param source_table: Contains the rows to be merged to the target_table
        :param target_table: Contains the destination table in which the rows will be merged
        :param source_to_target_columns_map: Dict of target_table columns names to source_table columns names
        :param target_conflict_columns: List of cols where we expect to have a conflict while combining
        :param if_conflicts: The strategy to be applied if there are conflicts.
        """
        raise NotImplementedError

    def get_sqla_table(self, table: Table) -> SqlaTable:
        """
        Return SQLAlchemy table instance

        :param table: Astro Table to be converted to SQLAlchemy table instance
        """
        return SqlaTable(
            table.name, table.sqlalchemy_metadata, autoload_with=self.sqlalchemy_engine
        )

    # ---------------------------------------------------------
    # Extract methods
    # ---------------------------------------------------------
    def export_table_to_pandas_dataframe(self, source_table: Table) -> pd.DataFrame:
        """
        Copy the content of a table to an in-memory Pandas dataframe.

        :param source_table: An existing table in the database
        """
        table_qualified_name = self.get_table_qualified_name(source_table)
        if self.table_exists(source_table):
            return pd.read_sql(
                # We are avoiding SQL injection by confirming the table exists before this statement
                f"SELECT * FROM {table_qualified_name}",  # skipcq BAN-B608
                con=self.sqlalchemy_engine,
            )
        raise NonExistentTableException(
            "The table %s does not exist" % table_qualified_name
        )

    def export_table_to_file(
        self,
        source_table: Table,
        target_file: File,
        if_exists: ExportExistsStrategy = "exception",
    ) -> None:
        """
        Copy the content of a table to a target file of supported type, in a supported location.

        :param source_table: An existing table in the database
        :param target_file: The path to the file to which we aim to dump the content of the database
        :param if_exists: Overwrite file if exists. Default False
        """
        if if_exists == "exception" and target_file.exists():
            raise FileExistsError(f"The file {target_file} already exists.")

        df = self.export_table_to_pandas_dataframe(source_table)
        target_file.create_from_dataframe(df)

    # ---------------------------------------------------------
    # Schema Management
    # ---------------------------------------------------------

    def create_schema_if_needed(self, schema: Optional[str]) -> None:
        """
        This function checks if the expected schema exists in the database. If the schema does not exist,
        it will attempt to create it.

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        # We check if the schema exists first because snowflake will fail on a create schema query even if it
        # doesn't actually create a schema.
        if schema and not self.schema_exists(schema):
            statement = self._create_schema_statement.format(schema)
            self.run_sql(statement)

    def schema_exists(self, schema: str) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        raise NotImplementedError

    # ---------------------------------------------------------
    # Context & Template Rendering methods (Transformations)
    # ---------------------------------------------------------

    def get_sqlalchemy_template_table_identifier_and_parameter(
        self, table: Table, jinja_table_identifier: str
    ) -> Tuple[str, str]:
        """
        During the conversion from a Jinja-templated SQL query to a SQLAlchemy query, there is the need to
        convert a Jinja table identifier to a safe SQLAlchemy-compatible table identifier.

        For example, the query:
            sql_statement = "SELECT * FROM {{input_table}};"
            parameters = {"input_table": Table(name="user_defined_table", metadata=Metadata(schema="some_schema"))}

        Can become (depending on the database):
            "SELECT * FROM some_schema.user_defined_table;"
            parameters = {"input_table": "user_defined_table"}

        Since the table value is templated, there is a safety concern (e.g. SQL injection).
        We recommend looking into the documentation of the database and seeing what are the best practices.
        For example, Snowflake:
        https://docs.snowflake.com/en/sql-reference/identifier-literal.html

        :param table: The table object we want to generate a safe table identifier for
        :param jinja_table_identifier: The name used within the Jinja template to represent this table
        :return: value to replace the table identifier in the query and the value that should be used to replace it
        """
        return (
            self.get_table_qualified_name(table),
            self.get_table_qualified_name(table),
        )
