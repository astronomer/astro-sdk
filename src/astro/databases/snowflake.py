"""Snowflake database implementation."""
import logging
import os
import random
import string
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlparse

import pandas as pd
from airflow.models import connection
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from pandas.io.sql import SQLDatabase
from snowflake.connector import pandas_tools

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    FileLocation,
    FileType,
    LoadExistStrategy,
    MergeConflictStrategy,
)
from astro.databases.base import BaseDatabase
from astro.files import File
from astro.files.types import get_filetype
from astro.sql.table import Metadata, Table

DEFAULT_CONN_ID = SnowflakeHook.default_conn_name


class SnowflakeDatabase(BaseDatabase):
    """
    Handle interactions with snowflake databases. If this class is successful, we should not have any snowflake-specific
    logic in other parts of our code-base.
    """

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        self.native_support: Dict[Any, str] = {
            FileLocation.GS: "gs_to_snowflake",
            FileLocation.S3: "s3_to_snowflake",
        }
        self.integration: Optional[str] = None
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
        snowflake_connection = self.hook.get_conn()
        return Metadata(
            schema=snowflake_connection.schema,
            database=snowflake_connection.database,
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

    def check_optimised_path(self, source_file: File, target_table: Table) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        """
        return bool(self.native_support.get(source_file.location.location_type))

    def optimised_transfer(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        **kwargs,
    ) -> None:
        """
        Checks if optimised path for transfer between File location to database exists
        and if it does, it transfers it and returns true else false.

        :param source_file: Source file
        :param target_table: Output target table on snowflake
        :param if_exists: Update strategy for file
        :param kwargs:
        """
        method_name = self.native_support.get(source_file.location.location_type)
        if method_name:
            transfer_method = self.__getattribute__(method_name)
            try:
                if transfer_method(
                    source_file=source_file,
                    target_table=target_table,
                    if_exists=if_exists,
                    **kwargs,
                ):
                    return
            except Exception as exec_err:
                raise exec_err
        return

    @staticmethod
    def get_gcs_project_id_from_conn(conn: connection) -> str:
        """
        Get GCS project id from conn

        :param conn: Airflow's connection
        """
        if conn.extra and conn.extra_dejson.get("project"):
            return str(conn.extra_dejson["project"])
        elif conn.host:
            return str(conn.host)
        raise ValueError(f"conn_id {conn.conn_id} has no project id.")

    def gs_to_snowflake(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        **kwargs,
    ) -> bool:
        """
        Checks if the table and schema exists on Snowflake.
        If table and schema exists, create stage based on integration passed by user.
        Generate query and run COPY INTO command and return True else returns False

        :param source_file: Source file
        :param target_table: Output target table on snowflake
        :param chunk_size: Chunk size for the file
        :param if_exists: Update strategy for file
        :param kwargs:
        :return: bool
        :return: bool
        """
        # Fetch snowflake integration passed by the user as part of output table.
        self.integration = target_table.optional_args.get("integration", None)
        if self.integration is None:
            logging.info(
                "Snowflake integration is not as optional_args in Table creation"
            )
            return False

        snowflake_hook = SnowflakeHook(snowflake_conn_id=self.conn_id)

        if if_exists == "replace":
            self.drop_table(target_table)
            if get_filetype(
                source_file.path
            ) == FileType.PARQUET and not self.table_exists(table=target_table):
                self.create_table_from_parquet(
                    source_file=source_file,
                    target_table=target_table,
                    snowflake_hook=snowflake_hook,
                )
            else:
                return False

        # check if snowflake table exists
        if self.table_exists(table=target_table):
            logging.info("Table exists.")
            stage_name = self.create_stage(
                source_file=source_file, snowflake_hook=snowflake_hook
            )
            sql_parts = self.create_copy_into_sql_parts(
                source_file=source_file, target_table=target_table, stage=stage_name
            )
            copy_query = "\n".join(sql_parts)

            logging.info("Executing COPY command...")
            execution_info = snowflake_hook.run(
                copy_query, autocommit=kwargs.get("autocommit", True)
            )
            logging.info("COPY command completed")
            logging.info(execution_info)
            self.drop_stage(stage_name=stage_name, snowflake_hook=snowflake_hook)
            return True
        return False

    def create_stage(self, source_file: File, snowflake_hook: SnowflakeHook) -> str:
        """
        Creates a stage name based on source_file path and storage integration provided.

        @param source_file: Source file
        @param snowflake_hook: Snowflake hook
        @return: Stage name created
        """
        # Here,
        # To copy dataset from gcs_path = gs://sample-bucket/workspace/sample.csv" to snowflake table
        # where stage is WORKSPACE_STAGE, following steps has to be completed.

        # The path of stage should be to the folder the file is in. Command to create STAGE would look
        # like following:
        # CREATE OR REPLACE STAGE <warehouse_name>.WORKSPACE_STAGE URL='gcs://sample-bucket/workspace'
        # FILE_FORMAT=(TYPE=CSV, TRIM_SPACE=TRUE) COPY_OPTIONS=(MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE)
        # storage_integration = <storage_integration_name>;

        # COPY INTO query would be:
        # COPY INTO sample_table from '@WORKSPACE_STAGE/sample.csv';
        FILE_FORMAT_MAPPING = {
            FileType.CSV: "CSV",
            FileType.PARQUET: "PARQUET",
            FileType.NDJSON: "JSON",
        }
        snowflake_query_file_storage_mapping = {"gs": "gcs", "s3": "s3"}
        stage_name = (
            "stage_"
            + random.choice(string.ascii_lowercase)
            + "".join(
                random.choice(string.ascii_lowercase + string.digits) for _ in range(7)
            )
        )

        parsed_url = urlparse(source_file.path, allow_fragments=False)
        bucket_type = snowflake_query_file_storage_mapping.get(
            parsed_url.scheme.lower(), "gcs"
        )

        location_url = (
            bucket_type + "://" + parsed_url.netloc + os.path.dirname(parsed_url.path)
        )
        if get_filetype(source_file.path) == FileType.CSV:
            # match_by_column_name option is not supported for file format CSV
            create_stage_sql = (
                "CREATE OR REPLACE STAGE {} URL='{}' FILE_FORMAT=(TYPE={}, TRIM_SPACE=TRUE) "
                "storage_integration = {};".format(
                    stage_name,
                    location_url,
                    FILE_FORMAT_MAPPING.get(get_filetype(source_file.path)),
                    self.integration,
                )
            )
        else:
            # match_by_column_name option is required for PARQUET and NDJSON formats
            create_stage_sql = (
                "CREATE OR REPLACE STAGE {} URL='{}' FILE_FORMAT=(TYPE={}, TRIM_SPACE=TRUE) "
                "COPY_OPTIONS=(MATCH_BY_COLUMN_NAME=CASE_INSENSITIVE) storage_integration = {};".format(
                    stage_name,
                    location_url,
                    FILE_FORMAT_MAPPING.get(get_filetype(source_file.path)),
                    self.integration,
                )
            )
        execution_info = snowflake_hook.run(create_stage_sql, autocommit=True)
        logging.info(execution_info)
        return stage_name

    @staticmethod
    def drop_stage(stage_name: str, snowflake_hook: SnowflakeHook) -> None:
        """
        Runs the snowflake query to drop stage if exists
        @param stage_name: Stage name
        @param snowflake_hook: Snowflake hook
        @return:
        """
        drop_stage_query = f"DROP STAGE IF EXISTS {stage_name};"
        execution_info = snowflake_hook.run(drop_stage_query, autocommit=True)
        logging.info(execution_info)

    def create_copy_into_sql_parts(
        self, source_file: File, target_table: Table, stage: str
    ) -> List:
        """
        Create SQL queries to COPY INTO from file location in snowflake
        """

        sql_parts = [
            f"COPY INTO {self.get_table_qualified_name(target_table)}",
            f"FROM @{stage}/{os.path.basename(source_file.path) or ''}",
        ]
        return sql_parts

    def create_table_from_parquet(
        self, source_file: File, target_table: Table, snowflake_hook: SnowflakeHook
    ) -> None:
        """
        Creates table on snowflake using infer_schema for PARQUET table
        @param snowflake_hook: Snowflake hook
        @param source_file: Source file
        @param target_table: Output Table
        @return:
        """
        # Create file format for PARQUET file
        file_format_query = (
            "CREATE OR REPLACE FILE FORMAT parquet_format type = parquet;"
        )
        execution_info = snowflake_hook.run(file_format_query, autocommit=True)
        logging.info(execution_info)

        stage_name = self.create_stage(
            source_file=source_file, snowflake_hook=snowflake_hook
        )

        create_table_from_infer_schema_query = (
            "CREATE OR REPLACE TABLE %s USING TEMPLATE (SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*)) FROM TABLE ("
            "INFER_SCHEMA (LOCATION=>'@%s/%s', FILE_FORMAT=>'parquet_format')));"
            % (
                self.get_table_qualified_name(target_table),
                stage_name,
                os.path.basename(source_file.path) or "",
            )
        )

        execution_info = snowflake_hook.run(
            create_table_from_infer_schema_query, autocommit=True
        )
        logging.info(execution_info)

        self.drop_stage(stage_name=stage_name, snowflake_hook=snowflake_hook)

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
    Because Snowflake does not allow using `Identifier` for inserts or updates, we need to make reasonable attempts to
    ensure that no one can perform a SQL injection using this method. The following method ensures that a string
    follows the expected identifier syntax https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
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
