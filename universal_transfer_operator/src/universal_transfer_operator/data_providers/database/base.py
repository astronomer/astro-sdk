from __future__ import annotations

from typing import TYPE_CHECKING, Any, Callable

import pandas as pd
import sqlalchemy

if TYPE_CHECKING:  # pragma: no cover
    from sqlalchemy.engine.cursor import CursorResult

import warnings

import attr
from airflow.hooks.dbapi import DbApiHook
from pandas.io.sql import SQLDatabase
from sqlalchemy.sql import ClauseElement
from sqlalchemy.sql.schema import Table as SqlaTable

from universal_transfer_operator.constants import (
    DEFAULT_CHUNK_SIZE,
    ColumnCapitalization,
    LoadExistStrategy,
    Location,
    TransferMode,
)
from universal_transfer_operator.data_providers.base import DataProviders
from universal_transfer_operator.data_providers.filesystem import resolve_file_path_pattern
from universal_transfer_operator.data_providers.filesystem.base import FileStream
from universal_transfer_operator.datasets.dataframe.pandas import PandasDataframe
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.settings import LOAD_TABLE_AUTODETECT_ROWS_COUNT, SCHEMA
from universal_transfer_operator.universal_transfer_operator import TransferIntegrationOptions
from universal_transfer_operator.utils import get_dataset_connection_type


class DatabaseDataProvider(DataProviders[Table]):
    """DatabaseProviders represent all the DataProviders interactions with Databases."""

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
    # In run_raw_sql operator decides if we want to return results directly or process them by handler provided
    IGNORE_HANDLER_IN_RUN_RAW_SQL: bool = False
    NATIVE_PATHS: dict[Any, Any] = {}
    DEFAULT_SCHEMA = SCHEMA

    def __init__(
        self,
        dataset: Table,
        transfer_mode: TransferMode,
        transfer_params: TransferIntegrationOptions = attr.field(
            factory=TransferIntegrationOptions,
            converter=lambda val: TransferIntegrationOptions(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.dataset = dataset
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping = set()
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}
        super().__init__(
            dataset=self.dataset, transfer_mode=self.transfer_mode, transfer_params=self.transfer_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

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

    @property
    def transport_params(self) -> dict | None:  # skipcq: PYL-R0201
        """Get credentials required by smart open to access files"""
        return None

    def run_sql(
        self,
        sql: str | ClauseElement = "",
        parameters: dict | None = None,
        handler: Callable | None = None,
        **kwargs,
    ) -> CursorResult:
        """
        Return the results to running a SQL statement.

        Whenever possible, this method should be implemented using Airflow Hooks,
        since this will simplify the integration with Async operators.

        :param sql: Contains SQL query to be run against database
        :param parameters: Optional parameters to be used to render the query
        :param autocommit: Optional autocommit flag
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
        if handler:
            return handler(result)
        return None

    def columns_exist(self, table: Table, columns: list[str]) -> bool:
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

    def get_sqla_table(self, table: Table) -> SqlaTable:
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

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        table_qualified_name = self.get_table_qualified_name(table)
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(inspector.dialect.has_table(self.connection, table_qualified_name))

    def check_if_transfer_supported(self, source_dataset: Table) -> bool:
        """
        Checks if the transfer is supported from source to destination based on source_dataset.

        :param source_dataset: Table present in the source location
        """
        source_connection_type = get_dataset_connection_type(source_dataset)
        return Location(source_connection_type) in self.transfer_mapping

    def read(self):
        """Read the dataset and write to local reference location"""
        yield self.export_table_to_pandas_dataframe()

    def write(self, source_ref: FileStream | pd.DataFrame):
        """
        Write the data from local reference location to the dataset.
        :param source_ref: Stream of data to be loaded into output table.
        """
        if isinstance(source_ref, FileStream):
            return self.load_file_to_table(input_file=source_ref.actual_file, output_table=self.dataset)
        elif isinstance(source_ref, pd.DataFrame):
            return self.load_dataframe_to_table(input_dataframe=source_ref, output_table=self.dataset)

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        raise NotImplementedError

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------
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
        self.dataset.conn_id.

        :return: a Metadata instance
        """
        raise NotImplementedError

    def populate_metadata(self):
        """
        Given a table, check if the table has metadata.
        If the metadata is missing, and the database has metadata, assign it to the table.
        If the table schema was not defined by the end, retrieve the user-defined schema.
        This method performs the changes in-place and also returns the table.

        :param table: Table to potentially have their metadata changed
        :return table: Return the modified table
        """

        if self.dataset.metadata and self.dataset.metadata.is_empty() and self.default_metadata:
            self.dataset.metadata = self.default_metadata
        if not self.dataset.metadata.schema:
            self.dataset.metadata.schema = self.DEFAULT_SCHEMA

    # ---------------------------------------------------------
    # Table creation & deletion methods
    # ---------------------------------------------------------
    def create_table_using_columns(self, table: Table) -> None:
        """
        Create a SQL table using the table columns.

        :param table: The table to be created.
        """
        if not table.columns:
            raise ValueError("To use this method, table.columns must be defined")

        metadata = table.sqlalchemy_metadata
        sqlalchemy_table = sqlalchemy.Table(table.name, metadata, *table.columns)
        metadata.create_all(self.sqlalchemy_engine, tables=[sqlalchemy_table])

    def is_native_autodetect_schema_available(  # skipcq: PYL-R0201
        self, file: File  # skipcq: PYL-W0613
    ) -> bool:
        """
        Check if native auto detection of schema is available.

        :param file: File used to check the file type of to decide
            whether there is a native auto detection available for it.
        """
        return False

    def create_table_using_native_schema_autodetection(
        self,
        table: Table,
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
        table: Table,
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

    def create_table(
        self,
        table: Table,
        file: File | None = None,
        dataframe: pd.DataFrame | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
        use_native_support: bool = True,
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
        elif use_native_support and file and self.is_native_autodetect_schema_available(file):
            self.create_table_using_native_schema_autodetection(table, file)
        else:
            self.create_table_using_schema_autodetection(
                table,
                file=file,
                dataframe=dataframe,
                columns_names_capitalization=columns_names_capitalization,
            )

    def create_table_from_select_statement(
        self,
        statement: str,
        target_table: Table,
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

    def drop_table(self, table: Table) -> None:
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
        table: Table,
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
                file,
                normalize_config=normalize_config,
                filetype=file.type.name,
                transfer_params=self.transfer_params,
                transfer_mode=self.transfer_mode,
            )
            self.create_table(
                table,
                # We only use the first file for inferring the table schema
                files[0],
                columns_names_capitalization=columns_names_capitalization,
                use_native_support=use_native_support,
            )

    def create_schema_and_table_if_needed_from_dataframe(
        self,
        table: Table,
        dataframe: pd.DataFrame,
        columns_names_capitalization: ColumnCapitalization = "original",
        if_exists: LoadExistStrategy = "replace",
        use_native_support: bool = True,
    ):
        """
        Creates the schema and table from dataframe
        :param table: Table to create
        :param file: File path and conn_id for object stores
        :param normalize_config: pandas json_normalize params config
        :param columns_names_capitalization:  determines whether to convert all columns to lowercase/uppercase
        :param if_exists:  Overwrite file if exists
        :param use_native_support: Use native support for data transfer if available on the destination
        """
        if if_exists == "replace":
            self.drop_table(table)
        self.create_schema_if_needed(table.metadata.schema)
        if if_exists == "replace" or not self.table_exists(table):
            self.create_table(
                table,
                dataframe=dataframe,
                columns_names_capitalization=columns_names_capitalization,
                use_native_support=use_native_support,
            )

    def fetch_all_rows(self, table: Table, row_limit: int = -1) -> list:
        """
        Fetches all rows for a table and returns as a list. This is needed because some
        databases have different cursors that require different methods to fetch rows

        :param row_limit: Limit the number of rows returned, by default return all rows.
        :param table: The table metadata needed to fetch the rows
        :return: a list of rows
        """
        statement = f"SELECT * FROM {self.get_table_qualified_name(table)}"  # skipcq: BAN-B608
        if row_limit > -1:
            statement = statement + f" LIMIT {row_limit}"  # skipcq: BAN-B608
        response = self.run_sql(statement, handler=lambda x: x.fetchall())
        return response  # type: ignore

    def load_file_to_table(
        self,
        input_file: File,
        output_table: Table,
        normalize_config: dict | None = None,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        columns_names_capitalization: ColumnCapitalization = "original",
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
        self.load_file_to_table_using_pandas(
            input_file=input_file,
            output_table=output_table,
            normalize_config=normalize_config,
            if_exists="append",
            chunk_size=chunk_size,
        )

    def load_dataframe_to_table(
        self,
        input_dataframe: pd.DataFrame,
        output_table: Table,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        columns_names_capitalization: ColumnCapitalization = "original",
    ):
        """
        Load content of dataframe in output_table.

        :param input_dataframe: dataframe
        :param output_table: Table to create
        :param if_exists: Overwrite file if exists
        :param chunk_size: Specify the number of records in each batch to be written at a time
        :param normalize_config: pandas json_normalize params config
        :param columns_names_capitalization: determines whether to convert all columns to lowercase/uppercase
            in the resulting dataframe
        """

        self.create_schema_and_table_if_needed_from_dataframe(
            table=output_table,
            dataframe=input_dataframe,
            columns_names_capitalization=columns_names_capitalization,
            if_exists=if_exists,
        )
        self.load_pandas_dataframe_to_table(
            input_dataframe,
            output_table,
            chunk_size=chunk_size,
            if_exists=if_exists,
        )

    def load_file_to_table_using_pandas(
        self,
        input_file: File,
        output_table: Table,
        normalize_config: dict | None = None,
        if_exists: LoadExistStrategy = "replace",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ):
        input_files = resolve_file_path_pattern(
            file=input_file,
            normalize_config=normalize_config,
            filetype=input_file.type.name,
            transfer_params=self.transfer_params,
            transfer_mode=self.transfer_mode,
        )

        for file in input_files:
            self.load_pandas_dataframe_to_table(
                self.get_dataframe_from_file(file),
                output_table,
                chunk_size=chunk_size,
                if_exists=if_exists,
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
        self._assert_not_empty_df(source_dataframe)

        source_dataframe.to_sql(
            self.get_table_qualified_name(target_table),
            con=self.sqlalchemy_engine,
            if_exists=if_exists,
            chunksize=chunk_size,
            method="multi",
            index=False,
        )

    @staticmethod
    def _assert_not_empty_df(df):
        """Raise error if dataframe empty

        param df: A dataframe
        """
        if df.empty:
            raise ValueError("Can't load empty dataframe")

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

    def check_schema_autodetection_is_supported(  # skipcq: PYL-R0201
        self, source_file: File  # skipcq: PYL-W0613
    ) -> bool:
        """
        Checks if schema autodetection is handled natively by the database. Return False by default.

        :param source_file: File from which we need to transfer data
        """
        return False

    def check_file_pattern_based_schema_autodetection_is_supported(  # skipcq: PYL-R0201
        self, source_file: File  # skipcq: PYL-W0613
    ) -> bool:
        """
        Checks if schema autodetection is handled natively by the database for file
        patterns and prefixes. Return False by default.

        :param source_file: File from which we need to transfer data
        """
        return False

    def row_count(self, table: Table):
        """
        Returns the number of rows in a table.

        :param table: table to count
        :return: The number of rows in the table
        """
        result = self.run_sql(
            f"select count(*) from {self.get_table_qualified_name(table)}",
            handler=lambda x: x.scalar(),  # skipcq: BAN-B608
        )
        return result

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
    # Extract methods
    # ---------------------------------------------------------

    def export_table_to_pandas_dataframe(self) -> pd.DataFrame:
        """
        Copy the content of a table to an in-memory Pandas dataframe.
        """

        if not self.table_exists(self.dataset):
            raise ValueError(f"The table {self.dataset.name} does not exist")

        sqla_table = self.get_sqla_table(self.dataset)
        df = pd.read_sql(sql=sqla_table.select(), con=self.sqlalchemy_engine)
        return PandasDataframe.from_pandas_df(df)
