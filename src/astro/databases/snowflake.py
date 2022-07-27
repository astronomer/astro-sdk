"""Snowflake database implementation."""
import logging
import os
import random
import string
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple

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
from astro.files import File
from astro.sql.table import Metadata, Table

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
            + "".join(
                random.choice(string.ascii_lowercase + string.digits) for _ in range(7)
            )
        )

    def set_url_from_file(self, file: File) -> None:
        """
        Given a file to be loaded/unloaded to from Snowflake, identifies its folder and
        sets as self.url.

        It is also responsbile for adjusting any path specific requirements for Snowflake.

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
        ValueError,
        AttributeError,
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

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)

    @property
    def hook(self) -> SnowflakeHook:
        """Retrieve Airflow hook to interface with the snowflake database."""
        return SnowflakeHook(snowflake_conn_id=self.conn_id)

    @property
    def sql_type(self) -> str:
        return "snowflake"

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

    @staticmethod
    def get_table_qualified_name(table: Table) -> str:  # skipcq: PYL-R0201
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
    # Snowflake stage methods
    # ---------------------------------------------------------

    @staticmethod
    def _create_stage_auth_sub_statement(
        file: File, storage_integration: Optional[str] = None
    ) -> str:
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
                raise ValueError(
                    "In order to create an stage for GCS, `storage_integration` is required."
                )
            elif file.location.location_type == FileLocation.S3:
                aws = file.location.hook.get_credentials()
                if aws.access_key and aws.secret_key:
                    auth = f"credentials=(aws_key_id='{aws.access_key}' aws_secret_key='{aws.secret_key}');"
                else:
                    raise ValueError(
                        "In order to create an stage for S3, one of the following is required: "
                        "* `storage_integration`"
                        "* AWS_KEY_ID and SECRET_KEY_ID"
                    )
        return auth

    def create_stage(
        self,
        file: File,
        storage_integration: Optional[str] = None,
        metadata: Optional[Metadata] = None,
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
        auth = self._create_stage_auth_sub_statement(
            file=file, storage_integration=storage_integration
        )

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
            logging.error(
                "Stage '%s' does not exist or not authorized.", stage.qualified_name
            )
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

    def create_table_using_schema_autodetection(
        self,
        table: Table,
        file: Optional[File] = None,
        dataframe: Optional[pd.DataFrame] = None,
        columns_names_capitalization: ColumnCapitalization = "lower",
    ) -> None:
        """
        Create a SQL table, automatically inferring the schema using the given file.

        :param table: The table to be created.
        :param file: File used to infer the new table columns.
        :param dataframe: Dataframe used to infer the new table columns if there is no file
        """

        # Snowflake don't expect mixed case col names like - 'Title' or 'Category'
        # we explicitly convert them to lower case, if not provided by user
        if columns_names_capitalization not in ["lower", "upper"]:
            columns_names_capitalization = "lower"

        if file:
            dataframe = file.export_to_dataframe(
                nrows=settings.LOAD_TABLE_AUTODETECT_ROWS_COUNT,
                columns_names_capitalization=columns_names_capitalization,
            )

        # Snowflake doesn't handle well mixed capitalisation of column name chars
        # we are handling this more gracefully in a separate PR
        super().create_table_using_schema_autodetection(table, dataframe=dataframe)

    def is_native_load_file_available(
        self, source_file: File, target_table: Table
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        """
        is_file_type_supported = (
            source_file.type.name in NATIVE_LOAD_SUPPORTED_FILE_TYPES
        )
        is_file_location_supported = (
            source_file.location.location_type in NATIVE_LOAD_SUPPORTED_FILE_LOCATIONS
        )
        return is_file_type_supported and is_file_location_supported

    def load_file_to_table_natively(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        native_support_kwargs: Optional[Dict] = None,
        **kwargs,
    ):
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
        stage = self.create_stage(
            file=source_file, storage_integration=storage_integration
        )

        table_name = self.get_table_qualified_name(target_table)
        file_path = os.path.basename(source_file.path) or ""
        sql_statement = (
            f"COPY INTO {table_name} FROM @{stage.qualified_name}/{file_path}"
        )
        self.hook.run(sql_statement)
        self.drop_stage(stage)

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
        self.create_table(target_table, dataframe=source_dataframe)

        self.table_exists(target_table)
        pandas_tools.write_pandas(
            conn=self.hook.get_conn(),
            df=source_dataframe,
            table_name=target_table.name,
            schema=target_table.metadata.schema,
            database=target_table.metadata.database,
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
        created_schemas = [
            x["SCHEMA_NAME"]
            for x in self.hook.run(
                "SELECT SCHEMA_NAME from information_schema.schemata WHERE LOWER(SCHEMA_NAME) = %(schema_name)s;",
                parameters={"schema_name": schema.lower()},
            )
        ]
        return len(created_schemas) == 1

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
        statement, params = self._build_merge_sql(
            source_table=source_table,
            target_table=target_table,
            source_to_target_columns_map=source_to_target_columns_map,
            target_conflict_columns=target_conflict_columns,
            if_conflicts=if_conflicts,
        )
        self.run_sql(sql_statement=statement, parameters=params)

    def _build_merge_sql(
        self,
        source_table: Table,
        target_table: Table,
        source_to_target_columns_map: Dict[str, str],
        target_conflict_columns: List[str],
        if_conflicts: MergeConflictStrategy = "exception",
    ):
        """Build the SQL statement for Merge operation"""
        # TODO: Simplify this function
        source_table_name = source_table.name
        target_table_name = target_table.name

        source_cols = source_to_target_columns_map.keys()
        target_cols = source_to_target_columns_map.values()

        (
            source_table_identifier,
            source_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(
            source_table, "source_table"
        )

        (
            target_table_identifier,
            target_table_param,
        ) = self.get_sqlalchemy_template_table_identifier_and_parameter(
            target_table, "target_table"
        )

        statement = (
            f"merge into {target_table_identifier} using {source_table_identifier} "
            + "on {merge_clauses}"
        )

        merge_target_dict = {
            f"merge_clause_target_{i}": f"{target_table_name}.{x}"
            for i, x in enumerate(target_conflict_columns)
        }
        merge_source_dict = {
            f"merge_clause_source_{i}": f"{source_table_name}.{x}"
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
                raise ValueError(
                    f"The identifier {v} is invalid. Please check to prevent SQL injection"
                )
        if if_conflicts == "update":
            statement += " when matched then UPDATE SET {merge_vals}"
            merge_statement = ",".join(
                [
                    f"{target_table_name}.{t}={source_table_name}.{s}"
                    for s, t in source_to_target_columns_map.items()
                ]
            )
            statement = statement.replace("{merge_vals}", merge_statement)
        statement += (
            " when not matched then insert({target_columns}) values ({append_columns})"
        )
        statement = statement.replace(
            "{target_columns}",
            ",".join(f"{target_table_name}.{t}" for t in target_cols),
        )
        statement = statement.replace(
            "{append_columns}",
            ",".join(f"{source_table_name}.{s}" for s in source_cols),
        )
        params = {
            **merge_target_dict,
            **merge_source_dict,
            "source_table": source_table_param,
            "target_table": target_table_param,
        }
        return statement, params


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
