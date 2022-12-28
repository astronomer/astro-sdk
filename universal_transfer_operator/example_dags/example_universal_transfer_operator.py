from datetime import datetime

from airflow import DAG
from constants import TransferMode
from datasets.file import File
from datasets.table import Table

from universal_transfer_operator import UniversalTransferOperator

with DAG(
    "example_universal_transfer_operator",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    transfer_non_native = UniversalTransferOperator(
        task_id="transfer_non_native",
        source_dataset=File("gs://uto-test/uto/", conn_id="google_cloud_default"),
        destination_dataset=File("s3://astro-sdk-test/uto/", conn_id="aws_default"),
    )

    transfer_fivetran_with_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_with_connector_id",
        source_dataset=File("s3://astro-sdk-test/uto/", conn_id="aws_default"),
        destination_dataset=Table(
            "snowflake://gp21411.us-east-1.snowflakecomputing.com/providers_fivetran_dev.s3.fivetran_ankit_test",
            conn_id="snowflake_default",
        ),
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params={
            "thirdparty_conn_id": "fivetran_default",
            "connector_id": "filing_muppet",
        },
    )

    transfer_fivetran_without_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_without_connector_id",
        source_dataset=File("s3://astro-sdk-test/uto/", conn_id="aws_default"),
        destination_dataset=Table(
            "snowflake://{account name}/{database}.{schema}.{table}", conn_id="snowflake_default"
        ),
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params={
            "thirdparty_conn_id": "fivetran_default",
            "group": {"name": "test_group"},
            "destination": {
                "service": "snowflake",
                "time_zone_offset": "-5",
                "region": "GCP_US_EAST4",
                "config": {
                    "host": "your-account.snowflakecomputing.com",
                    "port": 443,
                    "database": "fivetran",
                    "auth": "PASSWORD",
                    "user": "fivetran_user",
                    "password": "123456",
                },
            },
            "connector": {
                "service": "s3",
                "config": {
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
            },
        },
    )
