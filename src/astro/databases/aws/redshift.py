"""AWS Redshift table implementation."""
from typing import Dict, List, Optional

import pandas as pd
import sqlalchemy
from airflow.providers.amazon.aws.hooks.redshift_sql import RedshiftSQLHook
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from astro.constants import (
    DEFAULT_CHUNK_SIZE,
    FileLocation,
    FileType,
    LoadExistStrategy,
)
from astro.databases.base import BaseDatabase
from astro.exceptions import DatabaseCustomError
from astro.files import File
from astro.settings import REDSHIFT_SCHEMA
from astro.sql.table import Metadata, Table

DEFAULT_CONN_ID = RedshiftSQLHook.default_conn_name
NATIVE_PATHS_SUPPORTED_FILE_TYPES = {
    FileType.CSV: "CSV",
    FileType.JSON: "JSON 'auto'",
    FileType.PARQUET: "PARQUET",
}


class RedshiftDatabase(BaseDatabase):
    """
    Handle interactions with Redshift databases.
    """

    DEFAULT_SCHEMA = REDSHIFT_SCHEMA
    NATIVE_PATHS = {
        FileLocation.S3: "load_s3_file_to_table",
    }

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

    def is_native_load_file_available(
        self, source_file: File, target_table: Table
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
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        native_support_kwargs: Optional[Dict] = None,
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
        target_table: Table,
        native_support_kwargs: Optional[Dict] = None,
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

        iam_role = native_support_kwargs.pop("IAM_ROLE") if native_support_kwargs.get("IAM_ROLE") else None
        copy_options = " ".join(
            f"{key} '{value}'" if isinstance(value, str) else f"{key} {value}"
            for key, value in native_support_kwargs.items()
        )
        if iam_role:
            sql_statement = (f"COPY {table_name} "
                             f"FROM '{source_file.path}' "
                             f"IAM_ROLE '{iam_role}' "
                             f"{file_type} "
                             f"{copy_options}")
        else:
            aws_creds = source_file.location.hook.get_credentials()
            sql_statement = (f"COPY {table_name} "
                             f"FROM '{source_file.path}' "
                             f"CREDENTIALS 'aws_access_key_id={aws_creds.access_key};"
                             f"aws_secret_access_key={aws_creds.secret_key}' "
                             f"{file_type} "
                             f"{copy_options}")

        try:
            self.hook.run(sql_statement)
        except (ValueError, AttributeError) as exe:
            raise DatabaseCustomError from exe
