import os
from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.constants import FileType
from astro.files import File
from astro.table import Metadata, Table

s3_bucket = os.getenv("AWS_S3_BUCKET_SWGOH")

dag = DAG(
    dag_id="swgoh_bigquery_to_s3_test_dag",
    start_date=datetime(2023, 6, 9),
    schedule_interval=None,
)
with dag:
    stage = "dev"  # TODO: modify when in "prod"

    # bigquery_hook = BigQueryHook(gcp_conn_id='GCP_SWGOH_CONN_ID')

    aql.export_to_file(
        task_id="get_data",
        input_data=Table(
            conn_id="bigquery_new",  # This is your Google Bigquery connection name configured in Airflow
            name="imdb_movies",
            metadata=Metadata(schema="astro"),
        ),
        output_file=File(
            path="s3://tmp9/character_training.csv",
            conn_id="aws_conn",  # This is you AWS S3 connection name configured in Airflow
            filetype=FileType.CSV,
        ),
        if_exists="replace",
    )
