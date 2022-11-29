from datetime import datetime

from airflow import DAG
from transfers.datasets.base import UniversalDataset as Dataset
from transfers.universal_transfer_operator import UniversalTransferOperator

START_DATE = datetime(2022, 1, 1)
with DAG(
    "universal_transfer_operator_example",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:
    uto_task = UniversalTransferOperator(
        task_id="universal_transfer_operator",
        source_dataset=Dataset("gs://uto-test/uto/", conn_id="gcp_conn", extra={}),
        destination_dataset=Dataset("s3://astro-sdk-test/uto/", conn_id="aws_conn", extra={}),
    )
