"""
This Example DAG:
- List all files from a bigquery bucket for given connection and file path pattern
- Dynamically expand on the list of files i.e create n parallel task if there are n files in the list to

- List the rows of a bigquery table
- Dynamically expand on the list of rows to calculate average rating if rating is a valid number.
"""

# [START howto_operator_get_file_list]

import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from astro import sql as aql
from astro.files import get_file_list
from astro.sql import get_value_list
from astro.sql.operators.load_file import LoadFileOperator as LoadFile
from astro.table import Metadata, Table
from astro.test_dag import test_dag

GCS_BUCKET = os.getenv("GCS_BUCKET", "gs://dag-authoring/dynamic_task/")
ASTRO_GCP_CONN_ID = os.getenv("ASTRO_GCP_CONN_ID", "google_cloud_default")
ASTRO_BIGQUERY_DATASET = os.getenv("ASTRO_BIGQUERY_DATASET", "dag_authoring")
QUERY_STATEMENT = os.getenv(
    "ASTRO_BIGQUERY_DATASET",
    "SELECT rating FROM `astronomer-dag-authoring.dynamic_template.movie`",
)

with DAG(
    dag_id="example_dynamic_task_template",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=["airflow_version:2.3.0"],
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

    # [START howto_operator_get_value_list]
    @task
    def custom_task(rating_val):
        try:
            return float(rating_val[0])
        except ValueError:
            # If value is not valid then ignore it
            pass

    @task
    def avg_rating(rating_list):
        rating_list = [val for val in rating_list if val]
        return sum(rating_list) / len(rating_list)

    rating = custom_task.expand(
        rating_val=get_value_list(sql=QUERY_STATEMENT, conn_id=ASTRO_GCP_CONN_ID)
    )

    print(avg_rating(rating))
    # [END howto_operator_get_value_list]

    aql.cleanup()
if __name__ == "__main__":
    test_dag(dag)
