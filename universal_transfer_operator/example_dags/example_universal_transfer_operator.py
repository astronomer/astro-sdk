import os
from datetime import datetime

from airflow import DAG

from universal_transfer_operator.constants import FileType, TransferMode
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.integrations.fivetran import Connector, Destination, FiveTranOptions, Group
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

with DAG(
    "example_universal_transfer_operator",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    transfer_non_native_gs_to_s3 = UniversalTransferOperator(
        task_id="transfer_non_native_gs_to_s3",
        source_dataset=File(path="gs://uto-test/uto/", conn_id="google_cloud_default"),
        destination_dataset=File(path="s3://astro-sdk-test/uto/", conn_id="aws_default"),
    )

    transfer_non_native_s3_to_gs = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_gs",
        source_dataset=File(path="s3://astro-sdk-test/uto/", conn_id="aws_default"),
        destination_dataset=File(
            path="gs://uto-test/uto/",
            conn_id="google_cloud_default",
        ),
    )

    transfer_non_native_s3_to_sqlite = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_sqlite",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_s3_table", conn_id="sqlite_default"),
    )

    transfer_non_native_gs_to_sqlite = UniversalTransferOperator(
        task_id="transfer_non_native_gs_to_sqlite",
        source_dataset=File(
            path="gs://uto-test/uto/csv_files/", conn_id="google_cloud_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_gs_table", conn_id="sqlite_default"),
    )

    transfer_non_native_s3_to_snowflake = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_snowflake",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_s3_table", conn_id="snowflake_default"),
    )

    transfer_non_native_gs_to_snowflake = UniversalTransferOperator(
        task_id="transfer_non_native_gs_to_snowflake",
        source_dataset=File(
            path="gs://uto-test/uto/csv_files/", conn_id="google_cloud_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_gs_table", conn_id="snowflake_default"),
    )

    transfer_fivetran_with_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_with_connector_id",
        source_dataset=File(path="s3://astro-sdk-test/uto/", conn_id="aws_default"),
        destination_dataset=Table(name="fivetran_test", conn_id="snowflake_default"),
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params=FiveTranOptions(conn_id="fivetran_default", connector_id="filing_muppet"),
    )

    transfer_fivetran_without_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_without_connector_id",
        source_dataset=File(path="s3://astro-sdk-test/uto/", conn_id="aws_default"),
        destination_dataset=Table(
            name="fivetran_test",
            conn_id="snowflake_default",
            metadata=Metadata(
                database=os.environ["SNOWFLAKE_DATABASE"], schema=os.environ["SNOWFLAKE_SCHEMA"]
            ),
        ),
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params=FiveTranOptions(
            conn_id="fivetran_default",
            connector_id="filing_muppet",
            group=Group(name="test_group"),
            connector=Connector(
                service="s3",
                config={
                    "schema": "s3",
                    "append_file_option": "upsert_file",
                    "prefix": "folder_path",
                    "pattern": "file_pattern",
                    "escape_char": "",
                    "skip_after": 0,
                    "list_strategy": "complete_listing",
                    "bucket": "astro-sdk-test",
                    "empty_header": True,
                    "skip_before": 0,
                    "role_arn": "arn::your_role_arn",
                    "file_type": "csv",
                    "delimiter": "",
                    "is_public": False,
                    "on_error": "fail",
                    "compression": "bz2",
                    "table": "fivetran_test",
                    "archive_pattern": "regex_pattern",
                },
            ),
            destination=Destination(
                service="snowflake",
                time_zone_offset="-5",
                region="GCP_US_EAST4",
                config={
                    "host": "your-account.snowflakecomputing.com",
                    "port": 443,
                    "database": "fivetran",
                    "auth": "PASSWORD",
                    "user": "fivetran_user",
                    "password": "123456",
                },
            ),
        ),
    )
