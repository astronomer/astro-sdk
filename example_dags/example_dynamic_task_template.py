import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from astro import sql as aql
from astro.files import get_file_list
from astro.files.base import File
from astro.sql.table import Metadata, Table

GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://dag-authoring")
ASTRO_GCP_CONN_ID = os.getenv("ASTRO_GCP_CONN_ID", "google_cloud_default")
ASTRO_BIGQUERY_DATASET = os.getenv("ASTRO_BIGQUERY_DATASET", "dag_authoring")


# [START howto_operator_get_file_list]
@task
def load_to_bigquery(path):
    aql.load_file(
        input_file=File(path=path),
        output_table=Table(
            metadata=Metadata(
                schema=ASTRO_BIGQUERY_DATASET,
            ),
            conn_id=ASTRO_GCP_CONN_ID,
        ),
        use_native_support=False,
    )


with DAG(
    dag_id="example_dynamic_task_template",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    load_to_bigquery.expand(
        path=get_file_list(ASTRO_GCP_CONN_ID, f"{GCS_BUCKET}/*.csv")
    )

    # [END howto_operator_get_file_list]
    aql.cleanup()
