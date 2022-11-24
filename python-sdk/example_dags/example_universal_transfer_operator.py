from datetime import datetime

from airflow import DAG
from uto.datasets.base import UniversalDataset as Dataset
from uto.universal_transfer_operator import UniversalTransferOperator

START_DATE = datetime(2000, 1, 1)
with DAG(
    "uto_example",
    schedule_interval=None,
    start_date=START_DATE,
    catchup=False,
) as dag:
    uto_task = UniversalTransferOperator(
        task_id="uto",
        source_dataset=Dataset("gs://uto-test/uto/", conn_id="google_cloud_default"),
        destination_dataset=Dataset("s3://astro-sdk-test/uto/", conn_id="aws_default"),
        extras={},
        use_optimized_transfer=False,
        optimization_params={},
    )
