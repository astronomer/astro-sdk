from __future__ import annotations

from tempfile import NamedTemporaryFile

import attr
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from google.api_core.exceptions import (
    NotFound as GoogleNotFound,
)
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine

from universal_transfer_operator.constants import DEFAULT_CHUNK_SIZE, LoadExistStrategy
from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider, FileStream
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.settings import BIGQUERY_SCHEMA, BIGQUERY_SCHEMA_LOCATION
from universal_transfer_operator.universal_transfer_operator import TransferParameters


class BigqueryDataProvider(DatabaseDataProvider):
    """BigqueryDataProvider represent all the DataProviders interactions with Bigquery Databases."""

    DEFAULT_SCHEMA = BIGQUERY_SCHEMA

    illegal_column_name_chars: list[str] = ["."]
    illegal_column_name_chars_replacement: list[str] = ["_"]

    _create_schema_statement: str = "CREATE SCHEMA IF NOT EXISTS {} OPTIONS (location='{}')"

    def __init__(
        self,
        dataset: Table,
        transfer_mode,
        transfer_params: TransferParameters = attr.field(
            factory=TransferParameters,
            converter=lambda val: TransferParameters(**val) if isinstance(val, dict) else val,
        ),
    ):
        self.dataset = dataset
        self.transfer_params = transfer_params
        self.transfer_mode = transfer_mode
        self.transfer_mapping = {}
        self.LOAD_DATA_NATIVELY_FROM_SOURCE: dict = {}
        super().__init__(
            dataset=self.dataset, transfer_mode=self.transfer_mode, transfer_params=self.transfer_params
        )

    def __repr__(self):
        return f'{self.__class__.__name__}(conn_id="{self.dataset.conn_id})'

    @property
    def sql_type(self) -> str:
        return "bigquery"

    @property
    def hook(self) -> BigQueryHook:
        """Retrieve Airflow hook to interface with the Snowflake database."""
        return BigQueryHook(
            gcp_conn_id=self.dataset.conn_id, use_legacy_sql=False, location=BIGQUERY_SCHEMA_LOCATION
        )

    @property
    def sqlalchemy_engine(self) -> Engine:
        """Return SQAlchemy engine."""
        uri = self.hook.get_uri()
        with self.hook.provide_gcp_credential_file_as_context():
            return create_engine(uri)

    @property
    def default_metadata(self) -> Metadata:
        """
        Fill in default metadata values for table objects addressing snowflake databases
        """
        return Metadata(
            schema=self.DEFAULT_SCHEMA,
            database=self.hook.project_id,
        )  # type: ignore

    def read(self):
        """Read the dataset and write to local reference location"""

        with NamedTemporaryFile(mode="w", suffix=".parquet", delete=False) as tmp_file:
            df = self.export_table_to_pandas_dataframe()
            df.to_parquet(tmp_file.name)
            local_temp_file = FileStream(
                remote_obj_buffer=tmp_file.file,
                actual_filename=tmp_file.name,
                actual_file=File(path=tmp_file.name),
            )
            yield local_temp_file

    def write(self, source_ref: FileStream):
        """
        Write the data from local reference location to the dataset

        :param source_ref: Stream of data to be loaded into snowflake table.
        """
        return self.load_file_to_table(
            input_file=source_ref.actual_file,
            output_table=self.dataset,
        )

    # ---------------------------------------------------------
    # Table metadata
    # ---------------------------------------------------------

    def schema_exists(self, schema: str) -> bool:
        """
        Checks if a dataset exists in the bigquery

        :param schema: Bigquery namespace
        """
        try:
            self.hook.get_dataset(dataset_id=schema)
        except GoogleNotFound:
            # google.api_core.exceptions throws when a resource is not found
            return False
        return True

    def _get_schema_location(self, schema: str | None = None) -> str:
        """
        Get region where the schema is created

        :param schema: Bigquery namespace
        """
        if schema is None:
            return ""
        try:
            dataset = self.hook.get_dataset(dataset_id=schema)
            return str(dataset.location)
        except GoogleNotFound:
            # google.api_core.exceptions throws when a resource is not found
            return ""

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

        try:
            creds = self.hook._get_credentials()  # skipcq PYL-W021
        except AttributeError:
            # Details: https://github.com/astronomer/astro-sdk/issues/703
            creds = self.hook.get_credentials()
        source_dataframe.to_gbq(
            self.get_table_qualified_name(target_table),
            if_exists=if_exists,
            chunksize=chunk_size,
            project_id=self.hook.project_id,
            credentials=creds,
        )

    def create_schema_if_needed(self, schema: str | None) -> None:
        """
        This function checks if the expected schema exists in the database. If the schema does not exist,
        it will attempt to create it.

        :param schema: DB Schema - a namespace that contains named objects like (tables, functions, etc)
        """
        # We check if the schema exists first because BigQuery will fail on a create schema query even if it
        # doesn't actually create a schema.
        if schema and not self.schema_exists(schema):
            table_schema = self.dataset.metadata.schema if self.dataset and self.dataset.metadata else None
            table_location = self._get_schema_location(table_schema)

            location = table_location or BIGQUERY_SCHEMA_LOCATION
            statement = self._create_schema_statement.format(schema, location)
            self.run_sql(statement)

    def truncate_table(self, table):
        """Truncate table"""
        self.run_sql(f"TRUNCATE {self.get_table_qualified_name(table)}")

    @property
    def openlineage_dataset_name(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: db_name.schema_name.table_name
        """
        dataset = self.dataset.metadata.database or self.dataset.metadata.schema
        return f"{self.hook.project_id}.{dataset}.{self.dataset.name}"

    @property
    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: snowflake://ACCOUNT
        """
        return self.sql_type

    @property
    def openlineage_dataset_uri(self) -> str:
        """
        Returns the open lineage dataset uri as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        """
        return f"{self.openlineage_dataset_namespace()}{self.openlineage_dataset_name()}"
