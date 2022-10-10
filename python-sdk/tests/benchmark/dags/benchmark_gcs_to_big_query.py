"""
This DAG is to benchmark GCSToBigQueryOperator for various dataset
"""
import os
from datetime import datetime, timedelta

from airflow import models
from airflow.operators import bash_operator
from airflow.providers.google.cloud.operators.bigquery import BigQueryDeleteDatasetOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", "gcs_to_bq_benchmarking_dataset")
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", "gcs_to_bq_table")
GCP_CONN_ID = os.getenv("GCP_CONN_ID", "google_cloud_default")
EXECUTION_TIMEOUT_STR = os.getenv("EXECUTION_TIMEOUT_STR", default="4")
RETRIES_STR = os.getenv("DEFAULT_TASK_RETRIES", default="2")
DEFAULT_RETRY_DELAY_SECONDS_STR = os.getenv("DEFAULT_RETRY_DELAY_SECONDS", default="60")
EXECUTION_TIMEOUT = int(EXECUTION_TIMEOUT_STR)

default_args = {
    "execution_timeout": timedelta(hours=EXECUTION_TIMEOUT),
    "retries": int(RETRIES_STR),
    "retry_delay": timedelta(seconds=int(DEFAULT_RETRY_DELAY_SECONDS_STR)),
}

dag = models.DAG(
    dag_id="benchmark_gcs_to_bigquery_operator",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["benchmark", "dag_authoring"],
)
create_test_dataset = bash_operator.BashOperator(
    task_id="create_test_dataset",
    bash_command="bq mk --force=true %s" % DATASET_NAME,
    dag=dag,
)

load_ten_kb = GCSToBigQueryOperator(
    task_id="load_ten_kb",
    bucket="astro-sdk",
    source_objects=["benchmark/trimmed/covid_overview/covid_overview_10kb.parquet"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=None,
    source_format="PARQUET",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)
load_hundred_kb = GCSToBigQueryOperator(
    task_id="load_hundred_kb",
    bucket="astro-sdk",
    source_objects=["benchmark/trimmed/tate_britain/artist_data_100kb.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=None,
    source_format="CSV",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)
load_ten_mb = GCSToBigQueryOperator(
    task_id="load_ten_mb",
    bucket="astro-sdk",
    source_objects=["benchmark/trimmed/imdb/title_ratings_10mb.csv"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=None,
    source_format="CSV",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

load_one_gb = GCSToBigQueryOperator(
    task_id="load_one_gb",
    bucket="astro-sdk",
    source_objects=["benchmark/trimmed/stackoverflow/stackoverflow_posts_1g.ndjson"],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=None,
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

load_five_gb = GCSToBigQueryOperator(
    task_id="load_five_gb",
    bucket="astro-sdk",
    source_objects=[
        ("benchmark/trimmed/pypi/pypi-downloads-2021-03-28-0000000000" + str(i) + ".ndjson")
        if i >= 10
        else ("benchmark/trimmed/pypi/pypi-downloads-2021-03-28-0000000000" + "0" + str(i) + ".ndjson")
        for i in range(20)
    ],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=None,
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

delete_test_dataset = BigQueryDeleteDatasetOperator(
    task_id="delete_airflow_test_dataset",
    dataset_id=DATASET_NAME,
    delete_contents=True,
    dag=dag,
)
