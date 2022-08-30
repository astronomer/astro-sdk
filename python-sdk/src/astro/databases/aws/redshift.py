"""AWS Redshift table implementation."""
import random
import string
from typing import Dict, List, Optional

import pandas as pd
import sqlalchemy
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy, MergeConflictStrategy
from astro.databases.base import BaseDatabase
from astro.files import File
from astro.settings import REDSHIFT_SCHEMA
from astro.sql.table import Metadata, Table

DEFAULT_CONN_ID = RedshiftSQLHook.default_conn_name
UNIQUE_HASH_SIZE = 16


class RedshiftDatabase(BaseDatabase):
    """
    Handle interactions with Redshift databases.
    """

    DEFAULT_SCHEMA = REDSHIFT_SCHEMA

    illegal_column_name_chars: List[str] = ["."]
    illegal_column_name_chars_replacement: List[str] = ["_"]

    def __init__(self, conn_id: str = DEFAULT_CONN_ID):
        super().__init__(conn_id)
        self._create_table_statement: str = "CREATE TABLE {} AS {}"

    @property
    def sql_type(self):
        return "redshift"

    @property
    def hook(self) -> RedshiftSQLHook:
        """Retrieve Airflow hook to interface with the Redshift database."""
        return RedshiftSQLHook(redshift_conn_id=self.conn_id, use_legacy_sql=False)

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
        return Metadata(database=database, schema=self.DEFAULT_SCHEMA)

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

    def table_exists(self, table: Table) -> bool:
        """
        Check if a table exists in the database.

        :param table: Details of the table we want to check that exists
        """
        inspector = sqlalchemy.inspect(self.sqlalchemy_engine)
        return bool(
            inspector.dialect.has_table(
                self.connection, table.name, schema=table.metadata.schema
            )
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
        source_dataframe.to_sql(
            target_table.name,
            self.connection,
            index=False,
            schema=target_table.metadata.schema,
            if_exists=if_exists,
            chunksize=chunk_size,
        )

    @staticmethod
    def _get_conflict_statement(
        if_conflicts: MergeConflictStrategy,
        stage_table_name: str,
        target_table_name: str,
        target_conflict_columns: List[str],
    ):
        """
        Builds conflict SQL statement to be applied while merging.

        :param if_conflicts: the strategy to be applied if there are conflicts
        :param stage_table_name: name of the stage table created in Redshift for merge operation
        :param target_table_name: name of the target table in which data is to be merged
        :param target_conflict_columns: list of cols where we expect to have a conflict while combining
        """
        conflict_column = target_conflict_columns[0]
        conflict_statement: Optional[str] = None
        if if_conflicts == "ignore":
            conflict_statement = (
                f"DELETE FROM {stage_table_name} USING {target_table_name} "
                f"WHERE {stage_table_name}.{conflict_column}={target_table_name}.{conflict_column} "
            )
        elif if_conflicts == "update":
            conflict_statement = (
                f"DELETE FROM {target_table_name} USING {stage_table_name} "
                f"WHERE {stage_table_name}.{conflict_column}={target_table_name}.{conflict_column} "
            )
        if conflict_statement:
            for conflict_column in target_conflict_columns[1:]:
                conflict_statement += f" AND {stage_table_name}.{conflict_column}={target_table_name}.{conflict_column}"
        return conflict_statement

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
        source_schema = source_table.metadata.schema
        source_table_name = self.get_table_qualified_name(source_table)
        target_schema = target_table.metadata.schema
        target_table_name = self.get_table_qualified_name(target_table)
        stage_table_name = self.get_table_qualified_name(Table(metadata=Metadata(schema=DEFAULT_SCHEMA)))

        begin_transaction = "BEGIN TRANSACTION"
        create_temp_table = f"CREATE TEMP TABLE {stage_table_name} (LIKE {target_table_name})"

        source_columns = list(source_to_target_columns_map.keys())
        target_columns = list(source_to_target_columns_map.values())
        target_column_names_string = ",".join(map(str, target_columns))
        source_column_names_string = ",".join(map(str, source_columns))
        insert_into_stage_table = (
            f"INSERT INTO {stage_table_name}({target_column_names_string}) "
            f"SELECT {source_column_names_string} FROM {source_table_name}"
        )

        conflict_statement: Optional[str] = self._get_conflict_statement(
            if_conflicts, stage_table_name, target_table_name, target_conflict_columns
        )

        insert_into_target_table = (
            f"INSERT INTO {target_table_name} SELECT * FROM {stage_table_name}"
        )
        drop_stage_table = f"DROP TABLE {stage_table_name}"
        end_transaction = "END TRANSACTION"

        statements = [begin_transaction, create_temp_table, insert_into_stage_table]
        if conflict_statement:
            statements.append(conflict_statement)
        statements.extend([insert_into_target_table, drop_stage_table, end_transaction])

        with self.hook.get_cursor() as cursor:
            for statement in statements:
                cursor.execute(statement)

    def is_native_load_file_available(
        self, source_file: File, target_table: Table
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        """
        return False
