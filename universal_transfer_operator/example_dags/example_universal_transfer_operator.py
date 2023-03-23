import os
import pathlib
from datetime import datetime

from airflow import DAG

from universal_transfer_operator.constants import FileType
from universal_transfer_operator.datasets.file.base import File
from universal_transfer_operator.datasets.table import Metadata, Table
from universal_transfer_operator.universal_transfer_operator import UniversalTransferOperator

s3_bucket = os.getenv("S3_BUCKET", "s3://astro-sdk-test")
gcs_bucket = os.getenv("GCS_BUCKET", "gs://uto-test")

CWD = pathlib.Path(__file__).parent
DATA_DIR = str(CWD) + "/../../data/"

with DAG(
    "example_universal_transfer_operator",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    transfer_non_native_gs_to_s3 = UniversalTransferOperator(
        task_id="transfer_non_native_gs_to_s3",
        source_dataset=File(path=f"{gcs_bucket}/uto/", conn_id="google_cloud_default"),
        destination_dataset=File(path=f"{s3_bucket}/uto/", conn_id="aws_default"),
    )

    transfer_non_native_s3_to_gs = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_gs",
        source_dataset=File(path=f"{s3_bucket}/uto/", conn_id="aws_default"),
        destination_dataset=File(
            path=f"{gcs_bucket}/uto/",
            conn_id="google_cloud_default",
        ),
    )

    transfer_non_native_s3_to_sqlite = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_sqlite",
        source_dataset=File(path=f"{s3_bucket}/uto/csv_files/", conn_id="aws_default", filetype=FileType.CSV),
        destination_dataset=Table(name="uto_s3_to_sqlite_table", conn_id="sqlite_default"),
    )

    transfer_non_native_gs_to_sqlite = UniversalTransferOperator(
        task_id="transfer_non_native_gs_to_sqlite",
        source_dataset=File(
            path=f"{gcs_bucket}/uto/csv_files/", conn_id="google_cloud_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_gs_to_sqlite_table", conn_id="sqlite_default"),
    )

    transfer_non_native_s3_to_snowflake = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_snowflake",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_s3_table_to_snowflake", conn_id="snowflake_conn"),
    )

    transfer_non_native_gs_to_snowflake = UniversalTransferOperator(
        task_id="transfer_non_native_gs_to_snowflake",
        source_dataset=File(
            path="gs://uto-test/uto/csv_files/", conn_id="google_cloud_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(name="uto_gs_to_snowflake_table", conn_id="snowflake_conn"),
    )

    transfer_non_native_gs_to_bigquery = UniversalTransferOperator(
        task_id="transfer_non_native_gs_to_bigquery",
        source_dataset=File(path="gs://uto-test/uto/homes_main.csv", conn_id="google_cloud_default"),
        destination_dataset=Table(
            name="uto_gs_to_bigquery_table",
            conn_id="google_cloud_default",
            metadata=Metadata(schema="astro"),
        ),
    )

    transfer_non_native_s3_to_bigquery = UniversalTransferOperator(
        task_id="transfer_non_native_s3_to_bigquery",
        source_dataset=File(
            path="s3://astro-sdk-test/uto/csv_files/", conn_id="aws_default", filetype=FileType.CSV
        ),
        destination_dataset=Table(
            name="uto_s3_to_bigquery_destination_table",
            conn_id="google_cloud_default",
            metadata=Metadata(schema="astro"),
        ),
    )

    transfer_non_native_bigquery_to_snowflake = UniversalTransferOperator(
        task_id="transfer_non_native_bigquery_to_snowflake",
        source_dataset=Table(
            name="uto_s3_to_bigquery_table",
            conn_id="google_cloud_default",
            metadata=Metadata(schema="astro"),
        ),
        destination_dataset=Table(
            name="uto_bigquery_to_snowflake_table",
            conn_id="snowflake_conn",
        ),
    )

    transfer_non_native_bigquery_to_sqlite = UniversalTransferOperator(
        task_id="transfer_non_native_bigquery_to_sqlite",
        source_dataset=Table(
            name="uto_s3_to_bigquery_table", conn_id="google_cloud_default", metadata=Metadata(schema="astro")
        ),
        destination_dataset=Table(name="uto_bigquery_to_sqlite_table", conn_id="sqlite_default"),
    )

    transfer_non_native_local_to_sftp = UniversalTransferOperator(
        task_id="transfer_non_native_local_to_sftp",
        source_dataset=File(path=f"{DATA_DIR}sample.csv", filetype=FileType.CSV),
        destination_dataset=File(path="sftp://upload/sample.csv", conn_id="sftp_conn", filetype=FileType.CSV),
    )
