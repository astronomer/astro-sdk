"""
This Example DAG:
- List all files from a bigquery bucket for given connection and file path pattern
- Dynamically expand on the list of files i.e create n parallel task if there are n files in the list to
"""

# [START howto_operator_get_file_list]

import os
from datetime import datetime

from airflow import DAG

from astro import sql as aql
from astro.files import get_file_list
from astro.sql.operators.load_file import LoadFileOperator as LoadFile
from astro.sql.table import Metadata, Table

GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://dag-authoring/dynamic_task/")
ASTRO_GCP_CONN_ID = os.getenv("ASTRO_GCP_CONN_ID", "google_cloud_default")
ASTRO_BIGQUERY_DATASET = os.getenv("ASTRO_BIGQUERY_DATASET", "dag_authoring")


with DAG(
    dag_id="example_dynamic_task_template",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    LoadFile.partial(
        task_id="load_gcs_to_bq",
        output_table=Table(
            metadata=Metadata(
                schema=ASTRO_BIGQUERY_DATASET,
            ),
            conn_id=ASTRO_GCP_CONN_ID,
        ),
        use_native_support=True,
    ).expand(input_file=get_file_list(path=GCS_BUCKET, conn_id=ASTRO_GCP_CONN_ID))

    # [END howto_operator_get_file_list]
    aql.cleanup()
