import os
from datetime import datetime

from airflow import DAG

from universal_transfer_operator.constants import TransferMode
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.integrations.fivetran import Connector, Destination, FiveTranOptions, Group
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")

connector_config = {
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
}

destination_config = {
    "host": "your-account.snowflakecomputing.com",
    "port": 443,
    "database": "fivetran",
    "auth": "PASSWORD",
    "user": "fivetran_user",
    "password": "123456",
}

with DAG(
    "example_universal_transfer_operator",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    # [START fivetran_transfer_with_setup]
    transfer_fivetran_with_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_with_connector_id",
        source_dataset=File(path=f"{s3_bucket}/uto/", conn_id="aws_default"),
        destination_dataset=Table(name="fivetran_test", conn_id="snowflake_default"),
        transfer_mode=TransferMode.THIRDPARTY,
        transfer_params=FiveTranOptions(conn_id="fivetran_default", connector_id="filing_muppet"),
    )
    # [END fivetran_transfer_with_setup]

    transfer_fivetran_without_connector_id = UniversalTransferOperator(
        task_id="transfer_fivetran_without_connector_id",
        source_dataset=File(path=f"{s3_bucket}/uto/", conn_id="aws_default"),
        destination_dataset=Table(
            name="fivetran_test",
            conn_id="snowflake_conn",
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
                config=connector_config,
                connector_id=None,
                connect_card_config={"connector_val": "test_connector"},
            ),
            destination=Destination(
                service="snowflake",
                time_zone_offset="-5",
                region="GCP_US_EAST4",
                config=destination_config,
            ),
        ),
    )
