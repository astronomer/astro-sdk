"""AWS Redshift table implementation."""
from __future__ import annotations

from typing import Any

import pandas as pd
import sqlalchemy
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from redshift_connector.error import (
    ArrayContentNotHomogenousError,
    ArrayContentNotSupportedError,
    ArrayDimensionsNotConsistentError,
    DatabaseError,
    DataError,
    IntegrityError,
    InternalError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
)
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    FileLocation,
    FileType,
    LoadExistStrategy,
    MergeConflictStrategy,
)
from astro.databases.base import BaseDatabase
from astro.exceptions import DatabaseCustomError
from astro.files import File
from astro.settings import REDSHIFT_SCHEMA
from astro.table import BaseTable, Metadata, Table

DEFAULT_CONN_ID = RedshiftSQLHook.default_conn_name
NATIVE_PATHS_SUPPORTED_FILE_TYPES = {
    FileType.CSV: "CSV",
    # By default, COPY attempts to match all columns in the target table to JSON field name keys.
    # With this option, matching is case-sensitive. Column names in Amazon Redshift tables are always lowercase,
    # so when you use the 'auto ignorecase' option, matching JSON field names is case-insensitive.
    # Refer: https://docs.aws.amazon.com/redshift/latest/dg/copy-parameters-data-format.html#copy-json
    FileType.NDJSON: "JSON 'auto ignorecase'",
    FileType.PARQUET: "PARQUET",
}


class RedshiftDatabase(BaseDatabase):
    """
    Handle interactions with Redshift databases.
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
        ArrayContentNotSupportedError,
        ArrayContentNotHomogenousError,
        ArrayDimensionsNotConsistentError,
    )
    DEFAULT_SCHEMA = REDSHIFT_SCHEMA
    NATIVE_PATHS = {
        FileLocation.S3: "load_s3_file_to_table",
    }

    illegal_column_name_chars: list[str] = ["."]
    illegal_column_name_chars_replacement: list[str] = ["_"]

    def __init__(self, conn_id: str = DEFAULT_CONN_ID, table: BaseTable | None = None):
        super().__init__(conn_id)
        self._create_table_statement: str = "CREATE TABLE {} AS {}"
        self.table = table

    @property
    def sql_type(self):
        return "redshift"

    @property
    def hook(self) -> RedshiftSQLHook:
        """Retrieve Airflow hook to interface with the Redshift database."""
        kwargs = {}
        _hook = RedshiftSQLHook(redshift_conn_id=self.conn_id, use_legacy_sql=False)
        database = _hook.conn.schema
        # currently, kwargs might not get used because Airflow hook accept it but does not use it.
        if (database is None) and (self.table and self.table.metadata and self.table.metadata.database):
            kwargs.update({"schema": self.table.metadata.database})
        return RedshiftSQLHook(redshift_conn_id=self.conn_id, use_legacy_sql=False, **kwargs)

    @property
    def sqlalchemy_engine(self) -> Engine:
        """Return SQAlchemy engine."""
        uri = self.hook.get_uri()
        return create_engine(uri)

    @property
    def default_metadata(self) -> Metadata:
        """Fill in default metadata values for table objects addressing redshift databases"""
        # TODO: Change airflow RedshiftSQLHook to fetch database and schema separately as it
        #  treats both of them the same way at the moment.
        database = self.hook.conn.schema
        return Metadata(database=database, schema=self.DEFAULT_SCHEMA)  # type: ignore

    def schema_exists(self, schema: str) -> bool:
        """
        Checks if a dataset exists in the Redshift

        :param schema: Redshift namespace
        """
        schema_result = self.hook.run(
            "SELECT schema_name FROM information_schema.schemata WHERE lower(schema_name) = lower(%s);",
            parameters={"schema_name": schema.lower()},
            handler=lambda x: [y[0] for y in x.fetchall()],
        )
        return len(schema_result) > 0

    def table_exists(self, table: BaseTable) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(inspector.dialect.has_table(self.connection, table.name, schema=table.metadata.schema))

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
            target_table.name,
            self.connection,
            index=False,
            schema=target_table.metadata.schema,
            if_exists=if_exists,
            chunksize=chunk_size,
        )

    @staticmethod
    def _get_conflict_statements(
        if_conflicts: MergeConflictStrategy,
        stage_table_name: str,
        source_table_name: str,
        source_to_target_map_source_columns: list[str],
        source_to_target_map_target_columns: list[str],
        source_table_all_columns_string: str,
        target_table_all_columns_string: str,
        target_conflict_columns: list[str],
    ) -> list[str] | None:
        """
        Builds conflict SQL statement to be applied while merging.

        :param if_conflicts: the strategy to be applied if there are conflicts
        :param stage_table_name: name of the stage table created in Redshift for merge operation
        :param source_table_name: name of the source table from which data is to be merged
        :param source_to_target_map_source_columns: list of source table columns built from the keys of provided
            source_to_target column map
        :param source_to_target_map_target_columns: list of target table columns built from the values of provided
            source_to_target column map
        :param source_table_all_columns_string: columns sequence to be used for the source table for fetching
        :param target_table_all_columns_string: columns sequence to be used in the target table for insertion
        :param target_conflict_columns: list of columns where we expect to have a conflict while combining
        """
        conflict_columns_str = ",".join(target_conflict_columns)
        insert_statement = (
            f"INSERT INTO {stage_table_name}({target_table_all_columns_string}) "
            f"SELECT {source_table_all_columns_string} FROM {source_table_name} "
            f"WHERE ({conflict_columns_str}) "
            f"NOT IN (SELECT {conflict_columns_str} FROM {stage_table_name})"
        )

        conflict_statements = None
        if if_conflicts == "ignore":
            conflict_statements = [insert_statement]
        elif if_conflicts == "update":
            conflict_column = target_conflict_columns[0]
            update_statement = (
                f"UPDATE {stage_table_name} "
                f"SET {source_to_target_map_target_columns[0]}="
                f"{source_table_name}.{source_to_target_map_source_columns[0]}"
            )
            for (source_column, target_column) in zip(
                source_to_target_map_source_columns[1:],
                source_to_target_map_target_columns[1:],
            ):
                update_statement += f", {target_column}={source_table_name}.{source_column} "
            update_statement += (
                f"FROM {source_table_name} "
                f"WHERE {stage_table_name}.{conflict_column}={source_table_name}.{conflict_column} "
            )
            for conflict_col in target_conflict_columns[1:]:
                update_statement += (
                    f"AND {stage_table_name}.{conflict_col}={source_table_name}.{conflict_col} "
                )
            conflict_statements = [update_statement, insert_statement]

        return conflict_statements

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
        source_table_name = self.get_table_qualified_name(source_table)
        target_table_name = self.get_table_qualified_name(target_table)
        stage_table_name = self.get_table_qualified_name(Table())

        source_to_target_map_source_columns = list(source_to_target_columns_map.keys())
        source_to_target_map_target_columns = list(source_to_target_columns_map.values())
        # Need skipcq because string concatenation in query,
        # We should consider using parameterized if possible
        source_table_all_columns = self.hook.run(  # skipcq BAN-B608
            f"select col_name from pg_get_cols('{source_table_name}') "
            f"cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);",
            handler=lambda x: [y[0] for y in x.fetchall()],
        )
        target_table_all_columns = self.hook.run(  # skipcq BAN-B608
            f"select col_name from pg_get_cols('{target_table_name}') "
            f"cols(view_schema name, view_name name, col_name name, col_type varchar, col_num int);",
            handler=lambda x: [y[0] for y in x.fetchall()],
        )
        target_table_all_columns_string = ",".join(map(str, source_to_target_map_target_columns))
        source_table_all_columns_string = ",".join(map(str, source_to_target_map_source_columns))
        for column in target_table_all_columns:
            if column not in source_to_target_map_target_columns and column in source_table_all_columns:
                target_table_all_columns_string += f",{column}"
                source_table_all_columns_string += f",{column}"

        begin_transaction = "BEGIN TRANSACTION"
        create_temp_table = f"CREATE TEMP TABLE {stage_table_name} (LIKE {target_table_name})"
        insert_into_stage_table = (
            f"INSERT INTO {stage_table_name}({target_table_all_columns_string}) "
            f"SELECT {target_table_all_columns_string} FROM {target_table_name}"
        )
        conflict_statements: list[str] | None = self._get_conflict_statements(
            if_conflicts,
            stage_table_name,
            source_table_name,
            source_to_target_map_source_columns,
            source_to_target_map_target_columns,
            source_table_all_columns_string,
            target_table_all_columns_string,
            target_conflict_columns,
        )
        truncate_target_table = f"TRUNCATE {target_table_name}"
        insert_into_target_table = f"INSERT INTO {target_table_name} SELECT * FROM {stage_table_name}"
        drop_stage_table = f"DROP TABLE {stage_table_name}"
        end_transaction = "END TRANSACTION"

        statements = [begin_transaction, create_temp_table, insert_into_stage_table]
        if conflict_statements:
            statements.extend(conflict_statements)
        statements.extend(
            [
                truncate_target_table,
                insert_into_target_table,
                drop_stage_table,
                end_transaction,
            ]
        )

        with self.hook.get_cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def is_native_load_file_available(
        self, source_file: File, target_table: BaseTable  # skipcq PYL-W0613
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        """
        file_type = NATIVE_PATHS_SUPPORTED_FILE_TYPES.get(source_file.type.name)
        location_type = self.NATIVE_PATHS.get(source_file.location.location_type)
        return bool(location_type and file_type)

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
        and if it does, it transfers it and returns true else false.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        :param if_exists: Overwrite file if exists. Default False
        :param native_support_kwargs: kwargs to be used by method involved in native support flow
        """
        method_name = self.NATIVE_PATHS.get(source_file.location.location_type)
        if method_name:
            transfer_method = self.__getattribute__(method_name)
            transfer_method(
                source_file=source_file,
                target_table=target_table,
                if_exists=if_exists,
                native_support_kwargs=native_support_kwargs,
                **kwargs,
            )
        else:
            raise DatabaseCustomError(
                f"No transfer performed since there is no optimised path "
                f"for {source_file.location.location_type} to bigquery."
            )

    def load_s3_file_to_table(
        self,
        source_file: File,
        target_table: BaseTable,
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):
        """
        Load content of multiple files in S3 to output_table in Redshift by:
        - Creating a table
        - Using the COPY command

        :param source_file: Source file that is used as source of data
        :param target_table: Table that will be created on the redshift
        :param if_exists: Overwrite table if exists. Default 'replace'
        :param native_support_kwargs: kwargs to be used by method involved in native support flow

        .. seealso::
            `Redshift official documentation on CREATE TABLE
            <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_TABLE_NEW.html>`_
            `Redshift official documentation on COPY
            <https://docs.aws.amazon.com/redshift/latest/dg/r_COPY.html>`_
        """
        native_support_kwargs = native_support_kwargs or {}

        table_name = self.get_table_qualified_name(target_table)
        file_type = NATIVE_PATHS_SUPPORTED_FILE_TYPES.get(source_file.type.name)

        iam_role = native_support_kwargs.pop("IAM_ROLE", None)
        if not iam_role:
            raise TypeError(
                "Expected argument `IAM_ROLE` not passed in `native_support_kwargs` needed for native load"
            )
        copy_options = " ".join(
            f"{key} '{value}'" if isinstance(value, str) else f"{key} {value}"
            for key, value in native_support_kwargs.items()
        )
        sql_statement = (
            f"COPY {table_name} "
            f"FROM '{source_file.path}' "
            f"IAM_ROLE '{iam_role}' "
            f"{file_type} "
            f"{copy_options}"
        )

        try:
            self.hook.run(sql_statement)
        except (ValueError, AttributeError) as exe:
            raise DatabaseCustomError from exe

    @staticmethod
    def get_merge_initialization_query(parameters: tuple) -> str:  # skipcq PYL-W0613
        """
        Handles database-specific logic to handle constraints
        for Redshift.
        """
        return "SELECT 1 + 1"

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: schema_name.table_name
        """
        conn = self.hook.get_connection(self.conn_id)
        return f"{conn.schema}.{table.name}"

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: redshift://cluster:5439
        """
        conn = self.hook.conn
        return f"{self.sql_type}://{conn.host}:{conn.port}"
