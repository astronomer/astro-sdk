"""
This Example DAG:
- Pull CSV from S3 and load in bigquery table
- Run select query on bigquery table
- Expand on the returned rows i.e if bigquery table contain n rows then
    n copy of ``summarize_campaign`` task will be created dynamically
    using dynamic task mapping
"""
import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task

from astro import sql as aql
from astro.files import File
from astro.sql import Table
from astro.sql.table import Metadata

ASTRO_BIGQUERY_DATASET = os.getenv("ASTRO_BIGQUERY_DATASET", "dag_authoring")
ASTRO_GCP_CONN_ID = os.getenv("ASTRO_GCP_CONN_ID", "google_cloud_default")
ASTRO_S3_BUCKET = os.getenv("S3_BUCKET", "s3://tmp9")


@task
def summarize_campaign(capaign_id: str):
    print(capaign_id)


def handle_result(result):
    return result.fetchall()


with DAG(
    dag_id="example_dynamic_map_task",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:

    @aql.run_raw_sql(handler=handle_result)
    def get_campaigns(table: Table):
        return """select id from {{table}}"""

    bq_table = aql.load_file(
        input_file=File(path=f"{ASTRO_S3_BUCKET}/ads.csv"),
        output_table=Table(
            metadata=Metadata(
                schema=ASTRO_BIGQUERY_DATASET,
            ),
            conn_id=ASTRO_GCP_CONN_ID,
        ),
        use_native_support=False,
    )

    summarize_campaign.expand(capaign_id=get_campaigns(bq_table))
    aql.cleanup()
