"""Google BigQuery table implementation."""
from __future__ import annotations

import time
from typing import Any, Callable, Mapping

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
from astro.settings import BIGQUERY_SCHEMA
from astro.table import BaseTable, Metadata

DEFAULT_CONN_ID = BigQueryHook.default_conn_name
NATIVE_PATHS_SUPPORTED_FILE_TYPES = {
    FileType.CSV: "CSV",
    FileType.NDJSON: "NEWLINE_DELIMITED_JSON",
    FileType.PARQUET: "PARQUET",
}
BIGQUERY_WRITE_DISPOSITION = {"replace": "WRITE_TRUNCATE", "append": "WRITE_APPEND"}


class BigqueryDatabase(BaseDatabase):
    """
    Handle interactions with Bigquery databases. If this class is successful, we should not have any Bigquery-specific
    logic in other parts of our code-base.
    """

    DEFAULT_SCHEMA = BIGQUERY_SCHEMA
    NATIVE_PATHS = {
        FileLocation.GS: "load_gs_file_to_table",
        FileLocation.S3: "load_s3_file_to_table",
        FileLocation.LOCAL: "load_local_file_to_table",
    }

    NATIVE_AUTODETECT_SCHEMA_CONFIG: Mapping[FileLocation, Mapping[str, list[FileType] | Callable]] = {
        FileLocation.GS: {
            "filetype": [FileType.CSV, FileType.NDJSON, FileType.PARQUET],
            "method": lambda table, file: None,
        },
    }

    FILE_PATTERN_BASED_AUTODETECT_SCHEMA_SUPPORTED: set[FileLocation] = {
        FileLocation.GS,
        FileLocation.LOCAL,
    }

    illegal_column_name_chars: list[str] = ["."]
    illegal_column_name_chars_replacement: list[str] = ["_"]
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

    def __init__(self, conn_id: str = DEFAULT_CONN_ID, table: BaseTable | None = None):
        super().__init__(conn_id)
        self.table = table

    @property
    def sql_type(self) -> str:
        return "bigquery"

    @property
    def hook(self) -> BigQueryHook:
        """Retrieve Airflow hook to interface with the BigQuery database."""
        return BigQueryHook(gcp_conn_id=self.conn_id, use_legacy_sql=False)

    @property
    def sqlalchemy_engine(self) -> Engine:
        """Return SQAlchemy engine."""
        uri = self.hook.get_uri()
        with self.hook.provide_gcp_credential_file_as_context():
            return create_engine(uri)

    @property
    def default_metadata(self) -> Metadata:
        """
        Fill in default metadata values for table objects addressing bigquery databases

        :return:
        """
        return Metadata(schema=self.DEFAULT_SCHEMA, database=self.hook.project_id)  # type: ignore

    def schema_exists(self, schema: str) -> bool:
        """
        Checks if a dataset exists in the BigQuery

        :param schema: Bigquery namespace
        """
        try:
            self.hook.get_dataset(dataset_id=schema)
        except GoogleNotFound:
            return False
        return True

    @staticmethod
    def get_merge_initialization_query(parameters: tuple) -> str:
        """
        Handles database-specific logic to handle constraints
        for BigQuery. The only constraint that BigQuery supports
        is NOT NULL.
        """
        return "RETURN"

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

        source_columns = list(source_to_target_columns_map.keys())
        target_columns = list(source_to_target_columns_map.values())

        target_table_name = self.get_table_qualified_name(target_table)
        source_table_name = self.get_table_qualified_name(source_table)

        insert_statement = f"INSERT ({', '.join(target_columns)}) VALUES ({', '.join(source_columns)})"
        merge_statement = (
            f"MERGE {target_table_name} T USING {source_table_name} S"
            f" ON {' AND '.join(f'T.{col}=S.{col}' for col in target_conflict_columns)}"
            f" WHEN NOT MATCHED BY TARGET THEN {insert_statement}"
        )
        if if_conflicts == "update":
            update_statement_map = ", ".join(
                f"T.{col}=S.{source_columns[idx]}" for idx, col in enumerate(target_columns)
            )
            if not self.columns_exist(source_table, source_columns):
                raise ValueError(f"Not all the columns provided exist for {source_table_name}!")
            if not self.columns_exist(target_table, target_columns):
                raise ValueError(f"Not all the columns provided exist for {target_table_name}!")
            # Note: Ignoring below sql injection warning, as we validate that the table columns exist beforehand.
            update_statement = f"UPDATE SET {update_statement_map}"  # skipcq BAN-B608
            merge_statement += f" WHEN MATCHED THEN {update_statement}"
        self.run_sql(sql=merge_statement)

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
        table: BaseTable,
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

    def load_gs_file_to_table(
        self,
        source_file: File,
        target_table: BaseTable,
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
        target_table: BaseTable,
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
        target_table: BaseTable,
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

    def openlineage_dataset_name(self, table: BaseTable) -> str:
        """
        Returns the open lineage dataset namespace as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: PROJECT.dataset_name.table_name
        """
        dataset = table.metadata.database or table.metadata.schema
        return f"{self.hook.project_id}.{dataset}.{table.name}"

    def openlineage_dataset_namespace(self) -> str:
        """
        Returns the open lineage dataset name as per
        https://github.com/OpenLineage/OpenLineage/blob/main/spec/Naming.md
        Example: bigquery
        """
        return self.sql_type


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
        target_table: BaseTable,
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
