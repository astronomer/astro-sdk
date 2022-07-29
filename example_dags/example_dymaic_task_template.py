import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from astro import dtt

S3_BUCKET = os.getenv("S3_BUCKET", "s3://tmp9")
GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://dag-authoring")


@task
def print_file_path(path):
    print(path)


with DAG(
    dag_id="example_dynamic_task_template",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    print_file_path.extend(path=dtt.get_file_list("aws_default", f"{S3_BUCKET}/*.csv"))

    print_file_path.extend(
        path=dtt.get_file_list("google_cloud_default", f"{GCS_BUCKET}/*.csv")
    )
