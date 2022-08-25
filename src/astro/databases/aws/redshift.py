"""AWS Redshift table implementation."""
from typing import List

import pandas as pd
import sqlalchemy
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from astro.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from astro.databases.base import BaseDatabase
from astro.files import File
from astro.settings import REDSHIFT_SCHEMA
from astro.sql.table import Metadata, Table

DEFAULT_CONN_ID = RedshiftSQLHook.default_conn_name


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
        # TODO: Change airflow RedshiftSQLHook to fetch database and schema separately.
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

    def is_native_load_file_available(
        self, source_file: File, target_table: Table
    ) -> bool:
        """
        Check if there is an optimised path for source to destination.

        :param source_file: File from which we need to transfer data
        :param target_table: Table that needs to be populated with file data
        """
        return False
