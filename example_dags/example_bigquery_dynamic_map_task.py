import os
from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryInsertJobOperator,
)

from astro import sql as aql
from astro.sql import Table
from astro.sql.table import Metadata

ASTRO_BIGQUERY_DATASET = os.getenv("ASTRO_BIGQUERY_DATASET", "dag_authoring")
ASTRO_GCP_LOCATION = os.getenv("ASTRO_GCP_LOCATION", "us")
ASTRO_GCP_CONN_ID = os.getenv("ASTRO_GCP_CONN_ID", "google_cloud_default")
ASTRO_BIGQUERY_SCHEMA = os.getenv("ASTRO_BIGQUERY_SCHEMA", "dag_authoring")
ASTRO_BIGQUERY_TABLE = os.getenv("ASTRO_BIGQUERY_TABLE", "campaigns")

ASTRO_BIGQUERY_SCHEMA_FIELDS = [
    {"name": "campaign_id", "type": "INTEGER", "mode": "REQUIRED"},
    {"name": "name", "type": "STRING", "mode": "NULLABLE"},
    {"name": "ds", "type": "STRING", "mode": "NULLABLE"},
]
INSERT_DATE = datetime.now().strftime("%Y-%m-%d")
INSERT_ROWS_QUERY = (
    f"INSERT {ASTRO_BIGQUERY_SCHEMA}.{ASTRO_BIGQUERY_TABLE} VALUES "
    f"(1, 'astro-release', '{INSERT_DATE}'), "
    f"(2, 'astro-demo', '{INSERT_DATE}');"
)


@task
def summarize_campaign(capaign_id: str):
    print(capaign_id)


def handle_result(result):
    print("result = ", result)
    return result.fetchall()


with DAG(
    dag_id="example_dynamic_map_task",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id="create_dataset",
        dataset_id=ASTRO_BIGQUERY_DATASET,
        location=ASTRO_GCP_LOCATION,
        gcp_conn_id=ASTRO_GCP_CONN_ID,
    )

    create_table = BigQueryCreateEmptyTableOperator(
        task_id="create_table",
        dataset_id=ASTRO_BIGQUERY_DATASET,
        table_id=ASTRO_BIGQUERY_TABLE,
        schema_fields=ASTRO_BIGQUERY_SCHEMA_FIELDS,
        location=ASTRO_GCP_LOCATION,
        gcp_conn_id=ASTRO_GCP_CONN_ID,
    )

    insert_query_job = BigQueryInsertJobOperator(
        task_id="insert_query_job",
        configuration={
            "query": {
                "query": INSERT_ROWS_QUERY,
                "useLegacySql": False,
            }
        },
        location=ASTRO_GCP_LOCATION,
        gcp_conn_id=ASTRO_GCP_CONN_ID,
    )

    bq_table = Table(
        name=ASTRO_BIGQUERY_TABLE,
        metadata=Metadata(
            schema=ASTRO_BIGQUERY_DATASET,
        ),
        conn_id=ASTRO_GCP_CONN_ID,
    )

    @aql.run_raw_sql(handler=handle_result)
    def get_campaigns(table: Table):
        return """select campaign_id from {{table}}"""

    delete_dataset = BigQueryDeleteDatasetOperator(
        task_id="delete_dataset",
        dataset_id=ASTRO_BIGQUERY_DATASET,
        delete_contents=True,
        gcp_conn_id=ASTRO_GCP_CONN_ID,
        trigger_rule="all_done",
    )

    t1 = get_campaigns(bq_table)
    summarize_campaign.expand(capaign_id=t1)
    create_dataset >> create_table >> insert_query_job >> t1 >> delete_dataset
