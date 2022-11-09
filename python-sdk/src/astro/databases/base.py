from __future__ import annotations

import logging
import warnings
from abc import ABC
from typing import TYPE_CHECKING, Any, Callable, Mapping

import pandas as pd
import sqlalchemy
from airflow.hooks.dbapi import DbApiHook
from pandas.io.sql import SQLDatabase
from sqlalchemy import column, insert, select

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.engine.cursor import CursorResult
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.elements import ColumnClause
from sqlalchemy.sql.schema import Table as SqlaTable

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    ColumnCapitalization,
    ExportExistsStrategy,
    FileLocation,
    FileType,
    LoadExistStrategy,
    MergeConflictStrategy,
)
from astro.exceptions import DatabaseCustomError, NonExistentTableException
from astro.files import File, resolve_file_path_pattern
from astro.files.types import create_file_type
from astro.files.types.base import FileType as FileTypeConstants
from astro.settings import LOAD_FILE_ENABLE_NATIVE_FALLBACK, LOAD_TABLE_AUTODETECT_ROWS_COUNT, SCHEMA
from astro.table import BaseTable, Metadata


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
    illegal_column_name_chars: list[str] = []
    illegal_column_name_chars_replacement: list[str] = []
    NATIVE_PATHS: dict[Any, Any] = {}
    NATIVE_LOAD_EXCEPTIONS: Any = DatabaseCustomError
    DEFAULT_SCHEMA = SCHEMA
    NATIVE_AUTODETECT_SCHEMA_CONFIG: Mapping[FileLocation, Mapping[str, list[FileType] | Callable]] = {}
    FILE_PATTERN_BASED_AUTODETECT_SCHEMA_SUPPORTED: set[FileLocation] = set()

    def __init__(self, conn_id: str):
        self.conn_id = conn_id
        self.sql: str | ClauseElement = ""

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
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        **kwargs,
    ) -> CursorResult:
        """
        Return the results to running a SQL statement.

        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        """
        if parameters is None:
            parameters = {}

        if "sql_statement" in kwargs:  # pragma: no cover
            warnings.warn(
                "`sql_statement` is deprecated and will be removed in future release"
                "Please use  `sql` param instead.",
                DeprecationWarning,
                stacklevel=2,
            )
            sql = kwargs.get("sql_statement")  # type: ignore

        # We need to autocommit=True to make sure the query runs. This is done exclusively for SnowflakeDatabase's
        # truncate method to reflect changes.
        if isinstance(sql, str):
            result = self.connection.execute(
                sqlalchemy.text(sql).execution_options(autocommit=True), parameters
            )
        else:
            result = self.connection.execute(sql, parameters)
        return result

    def columns_exist(self, table: BaseTable, columns: list[str]) -> bool:
        """
        Check that a list of columns exist in the given table.

        :param table: The table to check in.
        :param columns: The columns to check.

        :returns: whether the columns exist in the table or not.
        """
        sqla_table = self.get_sqla_table(table)
        return all(
            any(sqla_column.name == column for sqla_column in sqla_table.columns) for column in columns
        )

    def table_exists(self, table: BaseTable) -> bool:
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
    def get_merge_initialization_query(parameters: tuple) -> str:
        """
        Handles database-specific logic to handle constraints, keeping
        it agnostic to database.
        """
        constraints = ",".join(parameters)
        sql = "ALTER TABLE {{table}} ADD CONSTRAINT airflow UNIQUE (%s)" % constraints
        return sql

    @staticmethod
    def get_table_qualified_name(table: BaseTable) -> str:  # skipcq: PYL-R0201
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

    def populate_table_metadata(self, table: BaseTable) -> BaseTable:
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
            table.metadata.schema = self.DEFAULT_SCHEMA
        return table

    # ---------------------------------------------------------
    # Table creation & deletion methods
    # ---------------------------------------------------------
    def create_table_using_columns(self, table: BaseTable) -> None:
        """
        Create a SQL table using the table columns.

        :param table: The table to be created.
        """
        if not table.columns:
            raise ValueError("To use this method, table.columns must be defined")

        metadata = table.sqlalchemy_metadata
        sqlalchemy_table = sqlalchemy.Table(table.name, metadata, *table.columns)
        metadata.create_all(self.sqlalchemy_engine, tables=[sqlalchemy_table])

    def create_table_using_native_schema_autodetection(
        self,
        table: BaseTable,
        file: File,
    ) -> None:
        """
        Create a SQL table, automatically inferring the schema using the given file via native database support.

        :param table: The table to be created.
        :param file: File used to infer the new table columns.
        """
        raise NotImplementedError("Missing implementation of native schema autodetection.")

    def create_table_using_schema_autodetection(
        self,
        table: BaseTable,
        file: File | None = None,
        dataframe: pd.DataFrame | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",  # skipcq
    ) -> None:
        """
        Create a SQL table, automatically inferring the schema using the given file.

        :param table: The table to be created.
        :param file: File used to infer the new table columns.
        :param dataframe: Dataframe used to infer the new table columns if there is no file
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        if file is None:
            if dataframe is None:
                raise ValueError(
                    "File or Dataframe is required for creating table using schema autodetection"
                )
            source_dataframe = dataframe
        else:
            source_dataframe = file.export_to_dataframe(nrows=LOAD_TABLE_AUTODETECT_ROWS_COUNT)

        db = SQLDatabase(engine=self.sqlalchemy_engine)
        db.prep_table(
            source_dataframe,
            table.name.lower(),
            schema=table.metadata.schema,
            if_exists="replace",
            index=False,
        )

    def is_native_autodetect_schema_available(  # skipcq: PYL-R0201
        self, file: File  # skipcq: PYL-W0613
    ) -> bool:
        """
        Check if native auto detection of schema is available.

        :param file: File used to check the file type of to decide
            whether there is a native auto detection available for it.
        """
        return False

    def create_table(
        self,
        table: BaseTable,
        file: File | None = None,
        dataframe: pd.DataFrame | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
    ) -> None:
        """
        Create a table either using its explicitly defined columns or inferring
        it's columns from a given file.

        :param table: The table to be created
        :param file: (optional) File used to infer the table columns.
        :param dataframe: (optional) Dataframe used to infer the new table columns if there is no file
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """
        if table.columns:
            self.create_table_using_columns(table)
        elif file and self.is_native_autodetect_schema_available(file):
            self.create_table_using_native_schema_autodetection(table, file)
        else:
            self.create_table_using_schema_autodetection(table, file, dataframe, columns_names_capitalization)

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: BaseTable,
        parameters: dict | None = None,
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

    def drop_table(self, table: BaseTable) -> None:
        """
        Delete a SQL table, if it exists.

        :param table: The table to be deleted.
        """
        statement = self._drop_table_statement.format(self.get_table_qualified_name(table))
        self.run_sql(statement)

    # ---------------------------------------------------------
    # Table load methods
    # ---------------------------------------------------------

    def create_schema_and_table_if_needed(
        self,
        table: BaseTable,
        file: File,
        normalize_config: dict | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
        if_exists: LoadExistStrategy = "replace",
        use_native_support: bool = True,
    ):
        """
        Checks if the autodetect schema exists for native support else creates the schema and table
        :param table: Table to create
        :param file: File path and conn_id for object stores
        :param normalize_config: pandas json_normalize params config
        :param columns_names_capitalization:  determines whether to convert all columns to lowercase/uppercase
        :param if_exists:  Overwrite file if exists
        :param use_native_support: Use native support for data transfer if available on the destination
        """
        is_schema_autodetection_supported = self.check_schema_autodetection_is_supported(source_file=file)
        is_file_pattern_based_schema_autodetection_supported = (
            self.check_file_pattern_based_schema_autodetection_is_supported(source_file=file)
        )
        if if_exists == "replace":
            self.drop_table(table)
        if use_native_support and is_schema_autodetection_supported and not file.is_pattern():
            return
        if (
            use_native_support
            and file.is_pattern()
            and is_schema_autodetection_supported
            and is_file_pattern_based_schema_autodetection_supported
        ):
            return

        self.create_schema_if_needed(table.metadata.schema)
        if if_exists == "replace" or not self.table_exists(table):
            files = resolve_file_path_pattern(
                file.path,
                file.conn_id,
                normalize_config=normalize_config,
                filetype=file.type.name,
            )
            self.create_table(
                table,
                # We only use the first file for inferring the table schema
                files[0],
                columns_names_capitalization=columns_names_capitalization,
            )

    def load_file_to_table(
        self,
        input_file: File,
        output_table: BaseTable,
        normalize_config: dict | None = None,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        use_native_support: bool = True,
        native_support_kwargs: dict | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
        enable_native_fallback: bool | None = LOAD_FILE_ENABLE_NATIVE_FALLBACK,
        **kwargs,
    ):
        """
        Load content of multiple files in output_table.
        Multiple files are sourced from the file path, which can also be path pattern.

        :param input_file: File path and conn_id for object stores
        :param output_table: Table to create
        :param if_exists: Overwrite file if exists
        :param chunk_size: Specify the number of records in each batch to be written at a time
        :param use_native_support: Use native support for data transfer if available on the destination
        :param normalize_config: pandas json_normalize params config
        :param native_support_kwargs: kwargs to be used by method involved in native support flow
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer
        """
        normalize_config = normalize_config or {}

        self.create_schema_and_table_if_needed(
            file=input_file,
            table=output_table,
            columns_names_capitalization=columns_names_capitalization,
            if_exists=if_exists,
            normalize_config=normalize_config,
        )

        if use_native_support and self.is_native_load_file_available(
            source_file=input_file, target_table=output_table
        ):

            self.load_file_to_table_natively_with_fallback(
                source_file=input_file,
                target_table=output_table,
                if_exists="append",
                normalize_config=normalize_config,
                native_support_kwargs=native_support_kwargs,
                enable_native_fallback=enable_native_fallback,
                chunk_size=chunk_size,
            )
        else:
            self.load_file_to_table_using_pandas(
                input_file=input_file,
                output_table=output_table,
                normalize_config=normalize_config,
                if_exists="append",
                chunk_size=chunk_size,
            )

    @staticmethod
    def get_dataframe_from_file(file: File):
        """
        Get pandas dataframe file. We need export_to_dataframe() for Biqqery,Snowflake and Redshift except for Postgres.
        For postgres we are overriding this method and using export_to_dataframe_via_byte_stream().
        export_to_dataframe_via_byte_stream copies files in a buffer and then use that buffer to ingest data.
        With this approach we have significant performance boost for postgres.

        :param file: File path and conn_id for object stores
        """

        return file.export_to_dataframe()

    def load_file_to_table_using_pandas(
        self,
        input_file: File,
        output_table: BaseTable,
        normalize_config: dict | None = None,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        input_files = resolve_file_path_pattern(
            input_file.path,
            input_file.conn_id,
            normalize_config=normalize_config,
            filetype=input_file.type.name,
        )

        for file in input_files:
            self.load_pandas_dataframe_to_table(
                self.get_dataframe_from_file(file),
                output_table,
                chunk_size=chunk_size,
                if_exists=if_exists,
            )

    def load_file_to_table_natively_with_fallback(
        self,
        source_file: File,
        target_table: BaseTable,
        if_exists: LoadExistStrategy = "replace",
        normalize_config: dict | None = None,
        native_support_kwargs: dict | None = None,
        enable_native_fallback: bool | None = LOAD_FILE_ENABLE_NATIVE_FALLBACK,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        **kwargs,
    ):
        """
        Load content of a file in output_table.

        :param source_file: File path and conn_id for object stores
        :param target_table: Table to create
        :param if_exists: Overwrite file if exists
        :param chunk_size: Specify the number of records in each batch to be written at a time
        :param native_support_kwargs: kwargs to be used by method involved in native support flow
        :param enable_native_fallback: Use enable_native_fallback=True to fall back to default transfer
        :param normalize_config: pandas json_normalize params config
        """

        try:
            self.load_file_to_table_natively(
                source_file=source_file,
                target_table=target_table,
                if_exists=if_exists,
                native_support_kwargs=native_support_kwargs,
                **kwargs,
            )
        # Catching NATIVE_LOAD_EXCEPTIONS for fallback
        except self.NATIVE_LOAD_EXCEPTIONS as load_exception:  # skipcq: PYL-W0703
            logging.warning(
                "Loading files failed with Native Support. Falling back to Pandas-based load",
                exc_info=True,
            )
            if enable_native_fallback:
                self.load_file_to_table_using_pandas(
                    input_file=source_file,
                    output_table=target_table,
                    normalize_config=normalize_config,
                    if_exists=if_exists,
                    chunk_size=chunk_size,
                )
            else:
                raise load_exception

    def load_pandas_dataframe_to_table(
        self,
        source_dataframe: pd.DataFrame,
        target_table: BaseTable,
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
            self.get_table_qualified_name(target_table),
            con=self.sqlalchemy_engine,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )

    def append_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
    ) -> None:
        """
        Append the source table rows into a destination table.

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param source_to_target_columns_map: Dict of source_table columns names to target_table columns names
        """
        target_table_sqla = self.get_sqla_table(target_table)
        source_table_sqla = self.get_sqla_table(source_table)

        target_columns: list[ColumnClause]
        source_columns: list[ColumnClause]

        if not source_to_target_columns_map:
            target_columns = [column(col) for col in target_table_sqla.c.keys()]
            source_columns = target_columns
        else:
            target_columns = [column(col) for col in source_to_target_columns_map.values()]
            source_columns = [column(col) for col in source_to_target_columns_map.keys()]

        sel = select(source_columns).select_from(source_table_sqla)
        # TODO: We should fix the following Type Error
        # incompatible type List[ColumnClause[<nothing>]]; expected List[Column[Any]]
        self.sql = insert(target_table_sqla).from_select(target_columns, sel)  # type: ignore[arg-type]
        self.run_sql(sql=self.sql)

    def merge_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
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

    def get_sqla_table(self, table: BaseTable) -> SqlaTable:
        """
        Return SQLAlchemy table instance

        :param table: Astro Table to be converted to SQLAlchemy table instance
        """
        return SqlaTable(
            table.name,
            table.sqlalchemy_metadata,
            autoload_with=self.sqlalchemy_engine,
            extend_existing=True,
        )

    # ---------------------------------------------------------
    # Extract methods
    # ---------------------------------------------------------
    def export_table_to_pandas_dataframe(
        self, source_table: BaseTable, select_kwargs: dict | None = None
    ) -> pd.DataFrame:
        """
        Copy the content of a table to an in-memory Pandas dataframe.

        :param source_table: An existing table in the database
        :param select_kwargs: kwargs for select statement
        """
        select_kwargs = select_kwargs or {}

        if self.table_exists(source_table):
            sqla_table = self.get_sqla_table(source_table)
            return pd.read_sql(sql=sqla_table.select(**select_kwargs), con=self.sqlalchemy_engine)

        table_qualified_name = self.get_table_qualified_name(source_table)
        raise NonExistentTableException(f"The table {table_qualified_name} does not exist")

    def export_table_to_file(
        self,
        source_table: BaseTable,
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

    def create_schema_if_needed(self, schema: str | None) -> None:
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
        self, table: BaseTable, jinja_table_identifier: str
    ) -> tuple[str, str]:  # skipcq PYL-W0613
        """
        During the conversion from a Jinja-templated SQL query to a SQLAlchemy query, there is the need to
        convert a Jinja table identifier to a safe SQLAlchemy-compatible table identifier.

        For example, the query:
            sql = "SELECT * FROM {{input_table}};"
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

    def is_native_load_file_available(  # skipcq: PYL-R0201
        self, source_file: File, target_table: BaseTable  # skipcq: PYL-W0613
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        """
        return False

    def load_file_to_table_natively(
        self,
        source_file: File,
        target_table: BaseTable,
        if_exists: LoadExistStrategy = "replace",
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):
        """
        Checks if optimised path for transfer between File location to database exists
        and if it does, it transfers it and returns true else false

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        :param if_exists: Overwrite file if exists. Default False
        :param native_support_kwargs: kwargs to be used by native loading command
        """
        raise NotImplementedError

    def check_schema_autodetection_is_supported(self, source_file: File) -> bool:
        """
        Checks if schema autodetection is handled natively by the database

        :param source_file: File from which we need to transfer data
        """
        filetype_supported = self.NATIVE_AUTODETECT_SCHEMA_CONFIG.get(source_file.location.location_type)

        source_filetype = (
            source_file
            if isinstance(source_file.type, FileTypeConstants)
            else create_file_type(path=source_file.path, filetype=source_file.type)  # type: ignore
        )

        is_source_filetype_supported = (
            (source_filetype.type.name in filetype_supported.get("filetype"))  # type: ignore
            if filetype_supported
            else None
        )

        location_type = self.NATIVE_PATHS.get(source_file.location.location_type)
        return bool(location_type and is_source_filetype_supported)

    def check_file_pattern_based_schema_autodetection_is_supported(self, source_file: File) -> bool:
        """
        Checks if schema autodetection is handled natively by the database for file
        patterns and prefixes.

        :param source_file: File from which we need to transfer data
        """
        is_file_pattern_based_schema_autodetection_supported = (
            source_file.location.location_type in self.FILE_PATTERN_BASED_AUTODETECT_SCHEMA_SUPPORTED
        )
        return is_file_pattern_based_schema_autodetection_supported

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError
