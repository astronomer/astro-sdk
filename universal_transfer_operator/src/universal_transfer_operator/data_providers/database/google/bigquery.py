from __future__ import annotations

import time
from typing import Any, Callable, Mapping

import attr
import pandas as pd
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.google.cloud.hooks.bigquery_dts import BiqQueryDataTransferServiceHook
from google.api_core.exceptions import (
    ClientError,
    Conflict,
    Forbidden,
    GoogleAPIError,
    InvalidArgument,
    NotFound as GoogleNotFound,
    ResourceExhausted,
    RetryError,
    ServerError,
    ServiceUnavailable,
    TooManyRequests,
    Unauthorized,
    Unknown,
)
from google.cloud import bigquery, bigquery_datatransfer  # type: ignore
from google.cloud.bigquery_datatransfer_v1.types import (
    StartManualTransferRunsResponse,
    TransferConfig,
    TransferState,
)
from google.protobuf import timestamp_pb2  # type: ignore
from google.protobuf.struct_pb2 import Struct  # type: ignore
from google.resumable_media import InvalidResponse
from sqlalchemy import create_engine
from sqlalchemy.engine.base import Engine
from tenacity import retry, stop_after_attempt

from universal_transfer_operator.constants import DEFAULT_CHUNK_SIZE, FileType, LoadExistStrategy, Location
from universal_transfer_operator.data_providers.database.base import DatabaseDataProvider
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.exceptions import DatabaseCustomError
from universal_transfer_operator.settings import BIGQUERY_SCHEMA, BIGQUERY_SCHEMA_LOCATION
from universal_transfer_operator.universal_transfer_operator import TransferIntegrationOptions

DEFAULT_CONN_ID = BigQueryHook.default_conn_name
NATIVE_PATHS_SUPPORTED_FILE_TYPES = {
    FileType.CSV: "CSV",
    FileType.NDJSON: "NEWLINE_DELIMITED_JSON",
    FileType.PARQUET: "PARQUET",
}
BIGQUERY_WRITE_DISPOSITION = {"replace": "WRITE_TRUNCATE", "append": "WRITE_APPEND"}


class BigqueryDataProvider(DatabaseDataProvider):
    """BigqueryDataProvider represent all the DataProviders interactions with Bigquery Databases."""

    DEFAULT_SCHEMA = BIGQUERY_SCHEMA
    NATIVE_PATHS = {
        Location.GS: "load_gs_file_to_table",
        Location.S3: "load_s3_file_to_table",
        Location.LOCAL: "load_local_file_to_table",
    }
    NATIVE_AUTODETECT_SCHEMA_CONFIG: Mapping[Location, Mapping[str, list[FileType] | Callable]] = {
        Location.GS: {
            "filetype": [FileType.CSV, FileType.NDJSON, FileType.PARQUET],
            "method": lambda table, file: None,
        },
    }

    FILE_PATTERN_BASED_AUTODETECT_SCHEMA_SUPPORTED: set[Location] = {
        Location.GS,
        Location.LOCAL,
    }

    NATIVE_LOAD_EXCEPTIONS: Any = (
        GoogleNotFound,
        ClientError,
        GoogleAPIError,
        RetryError,
        InvalidArgument,
        Unauthorized,
        Forbidden,
        Conflict,
        TooManyRequests,
        ResourceExhausted,
        ServerError,
        Unknown,
        ServiceUnavailable,
        InvalidResponse,
        DatabaseCustomError,
    )

    illegal_column_name_chars: list[str] = ["."]
    illegal_column_name_chars_replacement: list[str] = ["_"]

    _create_schema_statement: str = "CREATE SCHEMA IF NOT EXISTS {} OPTIONS (location='{}')"

    def __init__(
        self,
        dataset: Table,
        transfer_mode,
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
    def sql_type(self) -> str:
        return "bigquery"

    @property
    def hook(self) -> BigQueryHook:
        """Retrieve Airflow hook to interface with Bigquery."""
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

    def is_native_autodetect_schema_available(  # skipcq: PYL-R0201
        self, file: File  # skipcq: PYL-W0613
    ) -> bool:
        """
        Check if native auto detection of schema is available.
        :param file: File used to check the file type of to decide
            whether there is a native auto detection available for it.
        """
        supported_config = self.NATIVE_AUTODETECT_SCHEMA_CONFIG.get(file.location.location_type)
        if supported_config and file.type.name in supported_config["filetype"]:  # type: ignore
            return True
        return False

    def create_table_using_native_schema_autodetection(  # skipcq: PYL-R0201
        self,
        table: Table,
        file: File,
    ) -> None:
        """
        Create a SQL table, automatically inferring the schema using the given file via native database support.
        :param table: The table to be created.
        :param file: File used to infer the new table columns.
        """
        supported_config = self.NATIVE_AUTODETECT_SCHEMA_CONFIG.get(file.location.location_type)
        return supported_config["method"](table=table, file=file)  # type: ignore

    # Require skipcq because method overriding we need param target_table
    def is_native_load_file_available(
        self, source_file: File, target_table: Table  # skipcq PYL-W0613
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

    def load_gs_file_to_table(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):
        """
        Transfer data from gcs to bigquery
        :param source_file: Source file that is used as source of data
        :param target_table: Table that will be created on the bigquery
        :param if_exists: Overwrite table if exists. Default 'replace'
        :param native_support_kwargs: kwargs to be used by method involved in native support flow
        """
        native_support_kwargs = native_support_kwargs or {}

        load_job_config = {
            "sourceUris": [source_file.path],
            "destinationTable": {
                "projectId": self.get_project_id(target_table),
                "datasetId": target_table.metadata.schema,
                "tableId": target_table.name,
            },
            "createDisposition": "CREATE_IF_NEEDED",
            "writeDisposition": BIGQUERY_WRITE_DISPOSITION[if_exists],
            "sourceFormat": NATIVE_PATHS_SUPPORTED_FILE_TYPES[source_file.type.name],
        }
        if self.is_native_autodetect_schema_available(file=source_file):
            load_job_config["autodetect"] = True  # type: ignore

        # TODO: Fix this -- it should be load_job_config.update(native_support_kwargs)
        native_support_kwargs.update(native_support_kwargs)

        # Since bigquery has other options besides used here, we need to expose them to end user.
        # https://cloud.google.com/bigquery/docs/reference/rest/v2/Job#JobConfigurationLoad
        load_job_config.update(kwargs)

        job_config = {
            "jobType": "LOAD",
            "load": load_job_config,
            "labels": {"target_table": target_table.name},
        }

        self.hook.insert_job(
            configuration=job_config,
        )

    def load_s3_file_to_table(
        self,
        source_file: File,
        target_table: Table,
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):
        """
        Load content of multiple files in S3 to output_table in Bigquery by using a datatransfer job
        Note - To use this function we need
        1. Enable API on Bigquery
        2. Enable Data transfer service on Bigquery, which is a chargeable service
        for more information refer - https://cloud.google.com/bigquery-transfer/docs/enable-transfer-service
        :param source_file: Source file that is used as source of data
        :param target_table: Table that will be created on the bigquery
        :param if_exists: Overwrite table if exists. Default 'replace'
        :param native_support_kwargs: kwargs to be used by method involved in native support flow
        """
        native_support_kwargs = native_support_kwargs or {}

        project_id = self.get_project_id(target_table)
        transfer = S3ToBigqueryDataTransfer(
            target_table=target_table,
            source_file=source_file,
            project_id=project_id,
            native_support_kwargs=native_support_kwargs,
            **kwargs,
        )
        transfer.run()

    def get_project_id(self, target_table) -> str:
        """
        Get project id from the hook.
        :param target_table: table object that the hook is derived from.
        """
        try:
            return str(self.hook.project_id)
        except AttributeError as exe:
            raise DatabaseCustomError(f"conn_id {target_table.conn_id} has no project id") from exe

    def load_local_file_to_table(
        self,
        source_file: File,
        target_table: Table,
        if_exists: LoadExistStrategy = "replace",
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):
        """Transfer data from local to bigquery"""
        native_support_kwargs = native_support_kwargs or {}
        # We need to maintain file_type to biqquery_format and not use NATIVE_PATHS_SUPPORTED_FILE_TYPES
        # because the load_table_from_file expects 'JSON' value for ndjson file.
        file_types_to_bigquery_format = {
            FileType.CSV: "CSV",
            FileType.NDJSON: "JSON",
            FileType.PARQUET: "PARQUET",
        }

        client = self.hook.get_client()
        config = {
            "source_format": file_types_to_bigquery_format[source_file.type.name],
            "create_disposition": "CREATE_IF_NEEDED",
            "write_disposition": BIGQUERY_WRITE_DISPOSITION[if_exists],
            "autodetect": True,
        }
        config.update(native_support_kwargs)
        job_config = bigquery.LoadJobConfig(**config)

        # Deepsource pointed out - OWASP Top 10 2021 Category A01 - Broken Access Control
        # and Category A05 - Security Misconfiguration. Which are not applicable in this
        # case since user is always using there credentials, so they can't impersonate
        # other user roles.

        # We are passing mode='rb' even for text files since Bigquery
        # complain and ask to open file in 'rb' mode
        with open(source_file.path, mode="rb") as file:  # skipcq: PTC-W6004
            job = client.load_table_from_file(
                file,
                job_config=job_config,
                destination=self.get_table_qualified_name(target_table),
            )
        job.result()

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
        return f"{self.openlineage_dataset_namespace}{self.openlineage_dataset_name}"


class S3ToBigqueryDataTransfer:
    """
    Create and run Datatransfer job from S3 to Bigquery
    :param source_file: Source file that is used as source of data
    :param target_table: Table that will be created on the bigquery
    :param project_id: Bigquery project id
    :param poll_duration: sleep duration between two consecutive job status checks. Unit - seconds. Default 1 sec.
    :param native_support_kwargs: kwargs to be used by method involved in native support flow
    """

    def __init__(
        self,
        target_table: Table,
        source_file: File,
        project_id: str,
        poll_duration: int = 1,
        native_support_kwargs: dict | None = None,
        **kwargs,
    ):
        self.client = BiqQueryDataTransferServiceHook(gcp_conn_id=target_table.conn_id)
        self.target_table = target_table
        self.source_file = source_file

        aws = source_file.location.hook.get_credentials()
        self.s3_access_key = aws.access_key
        self.s3_secret_key = aws.secret_key
        file_types_to_bigquery_format = {
            FileType.CSV: "CSV",
            FileType.NDJSON: "JSON",
            FileType.PARQUET: "PARQUET",
        }
        self.s3_file_type = file_types_to_bigquery_format.get(source_file.type.name)

        self.project_id = project_id
        self.poll_duration = poll_duration
        self.native_support_kwargs = native_support_kwargs
        self.kwargs = kwargs

    def run(self):
        """Algo to run S3 to Bigquery datatransfer"""
        transfer_config_id = self.create_transfer_config()
        try:
            # Manually run a transfer job using previously created transfer config
            run_id = self.run_transfer_now(transfer_config_id)

            # Poll Bigquery for status of transfer job
            run_info = self.get_transfer_info(run_id=run_id, transfer_config_id=transfer_config_id)

            # Note - Super set of states that indicate the job is running.
            # This needs to be a super set as this if we miss on any running state, code will go into infinite loop.
            running_states = [TransferState.PENDING, TransferState.RUNNING]

            while run_info.state in running_states:
                run_info = self.get_transfer_info(run_id=run_id, transfer_config_id=transfer_config_id)
                time.sleep(self.poll_duration)

            if run_info.state != TransferState.SUCCEEDED:
                raise DatabaseCustomError(run_info.error_status)
        finally:
            # delete transfer config created.
            self.delete_transfer_config(transfer_config_id)

    @staticmethod
    def get_transfer_config_id(config: TransferConfig) -> str:
        """Extract transfer_config_id from TransferConfig object"""
        # ToDo: Look for a native way to extract 'transfer_config_id'
        # name - 'projects/103191871648/locations/us/transferConfigs/6302bf19-0000-26cf-a568-94eb2c0a61ee'
        # We need extract transferConfigs which is at the end of string.
        tokens = config.name.split("transferConfigs/")
        return str(tokens[-1])

    @staticmethod
    def get_run_id(config: StartManualTransferRunsResponse) -> str:
        """Extract run_id from StartManualTransferRunsResponse object"""
        # ToDo: Look for a native way to extract 'run_id'
        # config.runs[0].name - "projects/103191871648/locations/us/
        # transferConfigs/62d38894-0000-239c-a4d8-089e08325b54/runs/62d6a4df-0000-2fad-8752-d4f547e68ef4'
        # We need extract transferConfigs which is at the end of string.
        tokens = config.runs[0].name.split("runs/")
        return str(tokens[-1])

    def create_transfer_config(self):
        """Create bigquery transfer config on cloud"""
        s3_params = {
            "destination_table_name_template": self.target_table.name,
            "data_path": self.source_file.path,
            "access_key_id": self.s3_access_key,
            "secret_access_key": self.s3_secret_key,
            "file_format": self.s3_file_type,
        }
        s3_params.update(self.native_support_kwargs)

        params = Struct()
        params.update(s3_params)

        transfer_config = bigquery_datatransfer.TransferConfig(
            name="s3_to_bigquery",
            display_name="s3_to_bigquery",
            data_source_id="amazon_s3",
            params=params,
            schedule_options={"disable_auto_scheduling": True},
            disabled=False,
            destination_dataset_id=self.target_table.metadata.schema,
        )
        response = self.client.create_transfer_config(
            transfer_config=transfer_config, project_id=self.project_id
        )
        return self.get_transfer_config_id(response)

    def delete_transfer_config(self, transfer_config_id: str):
        """Delete transfer config created on Google cloud"""
        self.client.delete_transfer_config(transfer_config_id=transfer_config_id)

    def run_transfer_now(self, transfer_config_id: str):
        """Run transfer job on Google cloud"""
        start_time = timestamp_pb2.Timestamp(seconds=int(time.time() + 10))
        run_info = self.client.start_manual_transfer_runs(
            transfer_config_id=transfer_config_id,
            project_id=self.project_id,
            requested_run_time=start_time,
        )
        return self.get_run_id(run_info)

    @retry(stop=stop_after_attempt(3))
    def get_transfer_info(self, run_id: str, transfer_config_id: str):
        """Get transfer job info"""
        return self.client.get_transfer_run(run_id=run_id, transfer_config_id=transfer_config_id)
