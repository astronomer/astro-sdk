"""Snowflake database implementation."""
from __future__ import annotations

import logging
import os
import random
import string
from dataclasses import dataclass, field
from typing import Any, Sequence

import pandas as pd
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector import pandas_tools
from snowflake.connector.errors import (
    DatabaseError,
    DataError,
    ForbiddenError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    RequestTimeoutError,
    ServiceUnavailableError,
)
from sqlalchemy import Column, column, insert, select
from sqlalchemy.types import VARCHAR

from astro import settings
from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    ColumnCapitalization,
    FileLocation,
    FileType,
    LoadExistStrategy,
    MergeConflictStrategy,
)
from astro.databases.base import BaseDatabase
from astro.exceptions import DatabaseCustomError
from astro.files import File
from astro.settings import LOAD_TABLE_AUTODETECT_ROWS_COUNT, SNOWFLAKE_SCHEMA
from astro.table import BaseTable, Metadata

DEFAULT_CONN_ID = SnowflakeHook.default_conn_name

ASTRO_SDK_TO_SNOWFLAKE_FILE_FORMAT_MAP = {
    FileType.CSV: "CSV",
    FileType.NDJSON: "JSON",
    FileType.PARQUET: "PARQUET",
}

COPY_OPTIONS = {
    FileType.CSV: "ON_ERROR=CONTINUE",
    FileType.NDJSON: "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE",
    FileType.PARQUET: "MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE",
}

DEFAULT_STORAGE_INTEGRATION = {
    FileLocation.S3: settings.SNOWFLAKE_STORAGE_INTEGRATION_AMAZON,
    FileLocation.GS: settings.SNOWFLAKE_STORAGE_INTEGRATION_GOOGLE,
}

NATIVE_LOAD_SUPPORTED_FILE_TYPES = (FileType.CSV, FileType.NDJSON, FileType.PARQUET)
NATIVE_LOAD_SUPPORTED_FILE_LOCATIONS = (FileLocation.GS, FileLocation.S3)

NATIVE_AUTODETECT_SCHEMA_SUPPORTED_FILE_TYPES = {FileType.PARQUET}
NATIVE_AUTODETECT_SCHEMA_SUPPORTED_FILE_LOCATIONS = {FileLocation.GS, FileLocation.S3}

COPY_INTO_COMMAND_FAIL_STATUS = "LOAD_FAILED"


@dataclass
class SnowflakeFileFormat:
    """
    Dataclass which abstracts properties of a Snowflake File Format.

    Snowflake File Formats are used to define the format of files stored in a stage.

    Example:

    .. code-block:: python

        snowflake_stage = SnowflakeFileFormat(
            name="file_format",
            file_type="PARQUET",
        )

    .. seealso::
        `Snowflake official documentation on file format creation
        <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html>`_
    """

    name: str = ""
    _name: str = field(init=False, repr=False, default="")
    file_type: str = ""

    @staticmethod
    def _create_unique_name() -> str:
        """
        Generate a valid Snowflake file format name.

        :return: unique file format name
        """
        return (
            "file_format_"
            + random.choice(string.ascii_lowercase)
            + "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(7))
        )

    def set_file_type_from_file(self, file: File) -> None:
        """
        Set Snowflake specific file format based on a given file.

        :param file: File to use for file type mapping.
        """
        self.file_type = ASTRO_SDK_TO_SNOWFLAKE_FILE_FORMAT_MAP[file.type.name]

    @property  # type: ignore
    def name(self) -> str:
        """
        Return either the user-defined name or auto-generated one.

        :return: file format name
        :sphinx-autoapi-skip:
        """
        if not self._name:
            self._name = self._create_unique_name()
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the file format name.

        :param value: File format name.
        """
        if not isinstance(value, property) and value != self._name:
            self._name = value


@dataclass
class SnowflakeStage:
    """
    Dataclass which abstracts properties of a Snowflake Stage.

    Snowflake Stages are used to loading tables and unloading data from tables into files.

    Example:

    .. code-block:: python

        snowflake_stage = SnowflakeStage(
            name="stage_name",
            url="gcs://bucket/prefix",
            metadata=Metadata(database="SNOWFLAKE_DATABASE", schema="SNOWFLAKE_SCHEMA"),
        )

    .. seealso::
            `Snowflake official documentation on stage creation
            <https://docs.snowflake.com/en/sql-reference/sql/create-stage.html>`_
    """

    name: str = ""
    _name: str = field(init=False, repr=False, default="")
    url: str = ""
    metadata: Metadata = field(default_factory=Metadata)

    @staticmethod
    def _create_unique_name() -> str:
        """
        Generate a valid Snowflake stage name.

        :return: unique stage name
        """
        return (
            "stage_"
            + random.choice(string.ascii_lowercase)
            + "".join(random.choice(string.ascii_lowercase + string.digits) for _ in range(7))
        )

    def set_url_from_file(self, file: File) -> None:
        """
        Given a file to be loaded/unloaded to from Snowflake, identifies its folder and
        sets as self.url.

        It is also responsible for adjusting any path specific requirements for Snowflake.

        :param file: File to be loaded/unloaded to from Snowflake
        """
        # the stage URL needs to be the folder where the files are
        # https://docs.snowflake.com/en/sql-reference/sql/create-stage.html#external-stage-parameters-externalstageparams
        url = file.path[: file.path.rfind("/") + 1]
        self.url = url.replace("gs://", "gcs://")

    @property  # type: ignore
    def name(self) -> str:
        """
        Return either the user-defined name or auto-generated one.

        :return: stage name
        :sphinx-autoapi-skip:
        """
        if not self._name:
            self._name = self._create_unique_name()
        return self._name

    @name.setter
    def name(self, value: str) -> None:
        """
        Set the stage name.

        :param value: Stage name.
        """
        if not isinstance(value, property) and value != self._name:
            self._name = value

    @property
    def qualified_name(self) -> str:
        """
        Return stage qualified name. In Snowflake, it is the database, schema and table

        :return: Snowflake stage qualified name (e.g. database.schema.table)
        """
        qualified_name_lists = [
            self.metadata.database,
            self.metadata.schema,
            self.name,
        ]
        qualified_name = ".".join(name for name in qualified_name_lists if name)
        return qualified_name


class SnowflakeDatabase(BaseDatabase):
    """
    Handle interactions with snowflake databases. If this class is successful, we should not have any snowflake-specific
    logic in other parts of our code-base.
    """

    NATIVE_LOAD_EXCEPTIONS: Any = (
        DatabaseCustomError,
        ProgrammingError,
        DatabaseError,
        OperationalError,
        DataError,
        InternalError,
        IntegrityError,
        DataError,
        NotSupportedError,
        ServiceUnavailableError,
        ForbiddenError,
        RequestTimeoutError,
    )
    DEFAULT_SCHEMA = SNOWFLAKE_SCHEMA

    def __init__(self, conn_id: str = DEFAULT_CONN_ID, table: BaseTable | None = None):
        super().__init__(conn_id)
        self.table = table

    @property
    def hook(self) -> SnowflakeHook:
        """Retrieve Airflow hook to interface with the snowflake database."""
        kwargs = {}
        _hook = SnowflakeHook(snowflake_conn_id=self.conn_id)
        if self.table and self.table.metadata:
            if _hook.database is None and self.table.metadata.database:
                kwargs.update({"database": self.table.metadata.database})
            if _hook.schema is None and self.table.metadata.schema:
                kwargs.update({"schema": self.table.metadata.schema})
        return SnowflakeHook(snowflake_conn_id=self.conn_id, **kwargs)

    @property
    def sql_type(self) -> str:
        return "snowflake"

    @property
    def default_metadata(self) -> Metadata:
        """
        Fill in default metadata values for table objects addressing snowflake databases
        """
        connection = self.hook.get_conn()
        return Metadata(  # type: ignore
            schema=connection.schema,
            database=connection.database,
        )

    @staticmethod
    def get_table_qualified_name(table: BaseTable) -> str:  # skipcq: PYL-R0201
        """
        Return table qualified name. In Snowflake, it is the database, schema and table

        :param table: The table we want to retrieve the qualified name for.
        """
        qualified_name_lists = [
            table.metadata.database,
            table.metadata.schema,
            table.name,
        ]
        qualified_name = ".".join(name for name in qualified_name_lists if name)
        return qualified_name

    # ---------------------------------------------------------
    # Snowflake file format methods
    # ---------------------------------------------------------

    def create_file_format(self, file: File) -> SnowflakeFileFormat:
        """
        Create a new named file format.

        :param file: File to use for file format creation.

        .. seealso::
            `Snowflake official documentation on file format creation
            <https://docs.snowflake.com/en/sql-reference/sql/create-file-format.html>`_
        """
        file_format = SnowflakeFileFormat()
        file_format.set_file_type_from_file(file)

        sql_statement = "".join(
            [
                f"CREATE OR REPLACE FILE FORMAT {file_format.name} ",
                f"TYPE={file_format.file_type} ",
            ]
        )

        self.run_sql(sql_statement)

        return file_format

    # ---------------------------------------------------------
    # Snowflake stage methods
    # ---------------------------------------------------------

    @staticmethod
    def _create_stage_auth_sub_statement(file: File, storage_integration: str | None = None) -> str:
        """
        Create authentication-related line for the Snowflake CREATE STAGE.
        Raise an exception if it is not defined.

        :param file: File to be copied from/to using stage
        :param storage_integration: Previously created Snowflake storage integration
        :return: String containing line to be used for authentication on the remote storage
        """
        storage_integration = storage_integration or DEFAULT_STORAGE_INTEGRATION.get(
            file.location.location_type
        )
        if storage_integration is not None:
            auth = f"storage_integration = {storage_integration};"
        else:
            if file.location.location_type == FileLocation.GS:
                raise DatabaseCustomError(
                    "In order to create an stage for GCS, `storage_integration` is required."
                )
            elif file.location.location_type == FileLocation.S3:
                aws = file.location.hook.get_credentials()
                if aws.access_key and aws.secret_key:
                    auth = f"credentials=(aws_key_id='{aws.access_key}' aws_secret_key='{aws.secret_key}');"
                else:
                    raise DatabaseCustomError(
                        "In order to create an stage for S3, one of the following is required: "
                        "* `storage_integration`"
                        "* AWS_KEY_ID and SECRET_KEY_ID"
                    )
        return auth

    def create_stage(
        self,
        file: File,
        storage_integration: str | None = None,
        metadata: Metadata | None = None,
    ) -> SnowflakeStage:
        """
        Creates a new named external stage to use for loading data from files into Snowflake
        tables and unloading data from tables into files.

        At the moment, the following ways of authenticating to the backend are supported:
        * Google Cloud Storage (GCS): using storage_integration, previously created
        * Amazon (S3): one of the following:
        (i) using storage_integration or
        (ii) retrieving the AWS_KEY_ID and AWS_SECRET_KEY from the Airflow file connection

        :param file: File to be copied from/to using stage
        :param storage_integration: Previously created Snowflake storage integration
        :param metadata: Contains Snowflake database and schema information
        :return: Stage created

        .. seealso::
            `Snowflake official documentation on stage creation
            <https://docs.snowflake.com/en/sql-reference/sql/create-stage.html>`_
        """
        auth = self._create_stage_auth_sub_statement(file=file, storage_integration=storage_integration)

        metadata = metadata or self.default_metadata
        stage = SnowflakeStage(metadata=metadata)
        stage.set_url_from_file(file)

        fileformat = ASTRO_SDK_TO_SNOWFLAKE_FILE_FORMAT_MAP[file.type.name]
        copy_options = COPY_OPTIONS[file.type.name]

        sql_statement = "".join(
            [
                f"CREATE OR REPLACE STAGE {stage.qualified_name} URL='{stage.url}' ",
                f"FILE_FORMAT=(TYPE={fileformat}, TRIM_SPACE=TRUE) ",
                f"COPY_OPTIONS=({copy_options}) ",
                auth,
            ]
        )

        self.run_sql(sql_statement)

        return stage

    def stage_exists(self, stage: SnowflakeStage) -> bool:
        """
        Checks if a Snowflake stage exists.

        :param: SnowflakeStage instance
        :return: True/False
        """
        sql_statement = f"DESCRIBE STAGE {stage.qualified_name}"
        try:
            self.hook.run(sql_statement)
        except ProgrammingError:
            logging.error("Stage '%s' does not exist or not authorized.", stage.qualified_name)
            return False
        return True

    def drop_stage(self, stage: SnowflakeStage) -> None:
        """
        Runs the snowflake query to drop stage if it exists.

        :param stage: Stage to be dropped
        """
        sql_statement = f"DROP STAGE IF EXISTS {stage.qualified_name};"
        self.hook.run(sql_statement, autocommit=True)

    # ---------------------------------------------------------
    # Table load methods
    # ---------------------------------------------------------

    def is_native_autodetect_schema_available(  # skipcq: PYL-R0201
        self, file: File  # skipcq: PYL-W0613
    ) -> bool:
        """
        Check if native auto detection of schema is available.

        :param file: File used to check the file type of to decide
            whether there is a native auto detection available for it.
        """
        is_file_type_supported = file.type.name in NATIVE_AUTODETECT_SCHEMA_SUPPORTED_FILE_TYPES
        is_file_location_supported = (
            file.location.location_type in NATIVE_AUTODETECT_SCHEMA_SUPPORTED_FILE_LOCATIONS
        )
        return is_file_type_supported and is_file_location_supported

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
        table_name = self.get_table_qualified_name(table)
        file_format = self.create_file_format(file)
        stage = self.create_stage(file)
        file_path = os.path.basename(file.path) or ""
        sql_statement = """
            create table identifier(%(table_name)s) using template (
                select array_agg(object_construct(*))
                from table(
                    infer_schema(
                        location=>%(location)s,
                        file_format=>%(file_format)s
                    )
                )
            );
        """
        self.hook.run(
            sql_statement,
            parameters={
                "table_name": table_name,
                "location": f"@{stage.qualified_name}/{file_path}",
                "file_format": file_format.name,
            },
        )

    @classmethod
    def use_quotes(cls, cols: Sequence[str]) -> bool:
        """
        With snowflake identifier we have two cases,

        1. When Upper/Mixed case col names are used
            We are required to preserver the text casing of the col names. By adding the quotes around identifier.
        2. When lower case col names are used
            We can use them as is

        This is done to be in sync with Snowflake SQLAlchemy dialect.
        https://docs.snowflake.com/en/user-guide/sqlalchemy.html#object-name-case-handling

        Snowflake stores all case-insensitive object names in uppercase text. In contrast, SQLAlchemy considers all
        lowercase object names to be case-insensitive. Snowflake SQLAlchemy converts the object name case during
        schema-level communication (i.e. during table and index reflection). If you use uppercase object names,
        SQLAlchemy assumes they are case-sensitive and encloses the names with quotes. This behavior will cause
        mismatches against data dictionary data received from Snowflake, so unless identifier names have been truly
        created as case sensitive using quotes (e.g. "TestDb"), all lowercase names should be used on the SQLAlchemy
        side.

        :param cols: list of columns
        """
        return any(col for col in cols if not col.islower() and not col.isupper())

    def create_table_using_schema_autodetection(
        self,
        table: BaseTable,
        file: File | None = None,
        dataframe: pd.DataFrame | None = None,
        columns_names_capitalization: ColumnCapitalization = "original",
    ) -> None:  # skipcq PYL-W0613
        """
        Create a SQL table, automatically inferring the schema using the given file.
        Overriding default behaviour and not using the `prep_table` since it doesn't allow the adding quotes.

        :param table: The table to be created.
        :param file: File used to infer the new table columns.
        :param dataframe: Dataframe used to infer the new table columns if there is no file
        """
        if file is None:
            if dataframe is None:
                raise ValueError(
                    "File or Dataframe is required for creating table using schema autodetection"
                )
            source_dataframe = dataframe
        else:
            source_dataframe = file.export_to_dataframe(nrows=LOAD_TABLE_AUTODETECT_ROWS_COUNT)

        # We are changing the case of table name to ease out on the requirements to add quotes in raw queries.
        # ToDO - Currently, we cannot to append using load_file to a table name which is having name in lower case.
        pandas_tools.write_pandas(
            conn=self.hook.get_conn(),
            df=source_dataframe,
            table_name=table.name.upper(),
            schema=table.metadata.schema,
            database=table.metadata.database,
            chunk_size=DEFAULT_CHUNK_SIZE,
            quote_identifiers=self.use_quotes(source_dataframe),
            auto_create_table=True,
        )
        # We are truncating since we only expect table to be created with required schema.
        # Since this method is used by both native and pandas path we cannot skip this step.
        self.truncate_table(table)

    def is_native_load_file_available(
        self, source_file: File, target_table: BaseTable  # skipcq PYL-W0613, PYL-R0201
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        """
        is_file_type_supported = source_file.type.name in NATIVE_LOAD_SUPPORTED_FILE_TYPES
        is_file_location_supported = (
            source_file.location.location_type in NATIVE_LOAD_SUPPORTED_FILE_LOCATIONS
        )
        return is_file_type_supported and is_file_location_supported

    def load_file_to_table_natively(
        self,
        source_file: File,
        target_table: BaseTable,
        if_exists: LoadExistStrategy = "replace",
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):  # skipcq PYL-W0613
        """
        Load the content of a file to an existing Snowflake table natively by:
        - Creating a Snowflake external stage
        - Using Snowflake COPY INTO statement

        Requirements:
        - The user must have permissions to create a STAGE in Snowflake.
        - If loading from GCP Cloud Storage, `native_support_kwargs` must define `storage_integration`
        - If loading from AWS S3, the credentials for creating the stage may be
        retrieved from the Airflow connection or from the `storage_integration`
        attribute within `native_support_kwargs`.

        :param source_file: File from which we need to transfer data
        :param target_table: Table to which the content of the file will be loaded to
        :param if_exists: Strategy used to load (currently supported: "append" or "replace")
        :param native_support_kwargs: may be used for the stage creation, as described above.

        .. seealso::
            `Snowflake official documentation on COPY INTO
            <https://docs.snowflake.com/en/sql-reference/sql/copy-into-table.html>`_
            `Snowflake official documentation on CREATE STAGE
            <https://docs.snowflake.com/en/sql-reference/sql/create-stage.html>`_

        """
        native_support_kwargs = native_support_kwargs or {}
        storage_integration = native_support_kwargs.get("storage_integration")
        stage = self.create_stage(file=source_file, storage_integration=storage_integration)

        table_name = self.get_table_qualified_name(target_table)
        file_path = os.path.basename(source_file.path) or ""
        sql_statement = f"COPY INTO {table_name} FROM @{stage.qualified_name}/{file_path}"

        # Below code is added due to breaking change in apache-airflow-providers-snowflake==3.2.0,
        # we need to pass handler param to get the rows. But in version apache-airflow-providers-snowflake==3.1.0
        # if we pass the handler provider raises an exception AttributeError
        try:
            rows = self.hook.run(sql_statement, handler=lambda cur: cur.fetchall())
        except AttributeError:
            try:
                rows = self.hook.run(sql_statement)
            except (AttributeError, ValueError) as exe:
                raise DatabaseCustomError from exe
        except ValueError as exe:
            raise DatabaseCustomError from exe

        self.evaluate_results(rows)
        self.drop_stage(stage)

    @staticmethod
    def evaluate_results(rows):
        """check the error state returned by snowflake when running `copy into` query."""
        if any(row["status"] == COPY_INTO_COMMAND_FAIL_STATUS for row in rows):
            raise DatabaseCustomError(rows)

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

        auto_create_table = False
        if if_exists == "replace" or not self.table_exists(target_table):
            auto_create_table = True

        # We are changing the case of table name to ease out on the requirements to add quotes in raw queries.
        # ToDO - Currently, we cannot to append using load_file to a table name which is having name in lower case.
        pandas_tools.write_pandas(
            conn=self.hook.get_conn(),
            df=source_dataframe,
            table_name=target_table.name.upper(),
            schema=target_table.metadata.schema,
            database=target_table.metadata.database,
            chunk_size=chunk_size,
            quote_identifiers=self.use_quotes(source_dataframe),
            auto_create_table=auto_create_table,
        )

    def get_sqlalchemy_template_table_identifier_and_parameter(
        self, table: BaseTable, jinja_table_identifier: str
    ) -> tuple[str, str]:  # skipcq PYL-R0201
        """
        During the conversion from a Jinja-templated SQL query to a SQLAlchemy query, there is the need to
        convert a Jinja table identifier to a safe SQLAlchemy-compatible table identifier.

        For Snowflake, the query:
            sql_statement = "SELECT * FROM {{input_table}};"
            parameters = {"input_table": Table(name="user_defined_table", metadata=Metadata(schema="some_schema"))}

        Will become
            "SELECT * FROM IDENTIFIER(:input_table);"
            parameters = {"input_table": "some_schema.user_defined_table"}

        Example of usage: ::

            jinja_table_identifier, jinja_table_parameter_value = \
                get_sqlalchemy_template_table_identifier_and_parameter(
                    Table(name="user_defined_table", metadata=Metadata(schema="some_schema"),
                    "input_table"
                )
            assert jinja_table_identifier == "IDENTIFIER(:input_table)"
            assert jinja_table_parameter_value == "some_schema.user_defined_table"

        Since the table value is templated, there is a safety concern (e.g. SQL injection).
        We recommend looking into the documentation of the database and seeing what are the best practices.


        :param table: The table object we want to generate a safe table identifier for
        :param jinja_table_identifier: The name used within the Jinja template to represent this table
        :return: value to replace the table identifier in the query and the value that should be used to replace it

        .. seealso::
            `Snowflake official documentation on literals
            <https://docs.snowflake.com/en/sql-reference/identifier-literal.html>`_
        """
        return (
            f"IDENTIFIER(:{jinja_table_identifier})",
            SnowflakeDatabase.get_table_qualified_name(table),
        )

    def schema_exists(self, schema: str) -> bool:
        """
        Checks if a schema exists in the database

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """

        # Below code is added due to breaking change in apache-airflow-providers-snowflake==3.2.0,
        # we need to pass handler param to get the rows. But in version apache-airflow-providers-snowflake==3.1.0
        # if we pass the handler provider raises an exception AttributeError 'sfid'.
        try:
            schemas = self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
                handler=lambda cur: cur.fetchall(),
            )
        except AttributeError:
            schemas = self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
            )

        created_schemas = [x["SCHEMA_NAME"] for x in schemas]
        return len(created_schemas) == 1

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
        statement, params = self._build_merge_sql(
            source_table=source_table,
            target_table=target_table,
            source_to_target_columns_map=source_to_target_columns_map,
            target_conflict_columns=target_conflict_columns,
            if_conflicts=if_conflicts,
        )
        self.run_sql(sql=statement, parameters=params)

    def _build_merge_sql(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
        target_conflict_columns: list[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ):
        """Build the SQL statement for Merge operation"""
        # TODO: Simplify this function
        source_table_name = source_table.name
        target_table_name = target_table.name

        source_cols = source_to_target_columns_map.keys()
        target_cols = source_to_target_columns_map.values()

        target_identifier_enclosure = ""
        if self.use_quotes(list(target_cols)):
            target_identifier_enclosure = '"'

        source_identifier_enclosure = ""
        if self.use_quotes(list(source_cols)):
            source_identifier_enclosure = '"'

        (
            source_table_identifier,
            source_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(source_table, "source_table")

        (
            target_table_identifier,
            target_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(target_table, "target_table")

        statement = (
            f"merge into {target_table_identifier} using {source_table_identifier} " + "on {merge_clauses}"
        )

        merge_target_dict = {
            f"merge_clause_target_{i}": f"{target_table_name}."
            f"{target_identifier_enclosure}{x}{target_identifier_enclosure}"
            for i, x in enumerate(target_conflict_columns)
        }
        merge_source_dict = {
            f"merge_clause_source_{i}": f"{source_table_name}."
            f"{source_identifier_enclosure}{x}{source_identifier_enclosure}"
            for i, x in enumerate(target_conflict_columns)
        }
        statement = statement.replace(
            "{merge_clauses}",
            " AND ".join(
                f"{wrap_identifier(k)}={wrap_identifier(v)}"
                for k, v in zip(merge_target_dict.keys(), merge_source_dict.keys())
            ),
        )

        values_to_check = [target_table_name, source_table_name]
        values_to_check.extend(source_cols)
        values_to_check.extend(target_cols)
        for v in values_to_check:
            if not is_valid_snow_identifier(v):
                raise DatabaseCustomError(
                    f"The identifier {v} is invalid. Please check to prevent SQL injection"
                )
        if if_conflicts == "update":
            statement += " when matched then UPDATE SET {merge_vals}"
            merge_statement = ",".join(
                [
                    f"{target_table_name}.{target_identifier_enclosure}{t}{target_identifier_enclosure}="
                    f"{source_table_name}.{source_identifier_enclosure}{s}{source_identifier_enclosure}"
                    for s, t in source_to_target_columns_map.items()
                ]
            )
            statement = statement.replace("{merge_vals}", merge_statement)
        statement += " when not matched then insert({target_columns}) values ({append_columns})"
        statement = statement.replace(
            "{target_columns}",
            ",".join(
                f"{target_table_name}.{target_identifier_enclosure}{t}{target_identifier_enclosure}"
                for t in target_cols
            ),
        )
        statement = statement.replace(
            "{append_columns}",
            ",".join(
                f"{source_table_name}.{source_identifier_enclosure}{s}{source_identifier_enclosure}"
                for s in source_cols
            ),
        )
        params = {
            **merge_target_dict,
            **merge_source_dict,
            "source_table": source_table_param,
            "target_table": target_table_param,
        }
        return statement, params

    def append_table(
        self,
        source_table: BaseTable,
        target_table: BaseTable,
        source_to_target_columns_map: dict[str, str],
    ) -> None:
        """
        Append the source table rows into a destination table.

        Overriding the base method since we need to add quotes around the identifiers for
         snowflake to preserve case of cols - Column(name=col, quote=True)

        :param source_table: Contains the rows to be appended to the target_table
        :param target_table: Contains the destination table in which the rows will be appended
        :param source_to_target_columns_map: Dict of source_table columns names to target_table columns names
        """
        target_table_sqla = self.get_sqla_table(target_table)
        source_table_sqla = self.get_sqla_table(source_table)
        use_quotes_target_table = self.use_quotes(target_table_sqla.columns.keys())
        use_quotes_source_table = self.use_quotes(source_table_sqla.columns.keys())
        target_columns: list[column]
        source_columns: list[column]

        if not source_to_target_columns_map:
            target_columns = [
                Column(name=col.name, quote=use_quotes_target_table, type_=col.type)
                for col in target_table_sqla.c.values()
            ]
            source_columns = target_columns
        else:
            # We are adding the VARCHAR in Column(name=col, quote=True, type_=VARCHAR) as a placeholder since the
            # Column object requires it. It has no effect on the final query generated.
            target_columns = [
                Column(name=col, quote=use_quotes_target_table, type_=VARCHAR)
                for col in source_to_target_columns_map.keys()
            ]
            source_columns = [
                Column(name=col, quote=use_quotes_source_table, type_=VARCHAR)
                for col in source_to_target_columns_map.keys()
            ]

        sel = select(source_columns).select_from(source_table_sqla)
        # TODO: We should fix the following Type Error
        # incompatible type List[ColumnClause[<nothing>]]; expected List[Column[Any]]
        sql = insert(target_table_sqla).from_select(target_columns, sel)  # type: ignore[arg-type]
        self.run_sql(sql=sql)

    @classmethod
    def get_merge_initialization_query(cls, parameters: tuple) -> str:
        """
        Handles database-specific logic to handle constraints, keeping
        it agnostic to database.
        """
        identifier_enclosure = ""
        if cls.use_quotes(parameters):
            identifier_enclosure = '"'

        constraints = ",".join([f"{identifier_enclosure}{p}{identifier_enclosure}" for p in parameters])
        sql = "ALTER TABLE {{table}} ADD CONSTRAINT airflow UNIQUE (%s)" % constraints  # skipcq PYL-C0209
        return sql

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: db_name.schema_name.table_name
        """
        conn = self.hook.get_connection(self.conn_id)
        conn_extra = conn.extra_dejson
        schema = conn_extra.get("schema") or conn.schema
        db = conn_extra.get("database")
        return f"{db}.{schema}.{table.name}"

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: snowflake://ACCOUNT
        """
        account = self.hook.get_connection(self.conn_id).extra_dejson.get("account")
        return f"{self.sql_type}://{account}"

    def truncate_table(self, table):
        """Truncate table"""
        self.run_sql(f"TRUNCATE {self.get_table_qualified_name(table)}")


def wrap_identifier(inp: str) -> str:
    return f"Identifier(:{inp})"


def is_valid_snow_identifier(name: str) -> bool:
    """
    Because Snowflake does not allow using `Identifier` for inserts or updates,
    we need to make reasonable attempts to ensure that no one can perform a SQL
    injection using this method.
    The following method ensures that a string follows the expected identifier syntax.

    .. seealso::
        `Snowflake official documentation on indentifiers syntax
        <https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html>`_

    """
    if not 1 <= len(name) <= 255:
        return False

    name_is_quoted = name[0] == '"'
    if name_is_quoted:
        if len(name) < 2 or name[-1] != '"':
            return False  # invalid because no closing quote

        return ensure_internal_quotes_closed(name)
    return ensure_only_valid_characters(name)


# test code to check for validate snowflake identifier
def ensure_internal_quotes_closed(name: str) -> bool:
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


def ensure_only_valid_characters(name: str) -> bool:
    if not (name[0].isalpha()) and name[0] != "_":
        return False
    for c in name[1:]:
        if not (c.isalpha() or c.isdigit() or c == "_" or c == "$"):
            return False
    return True
