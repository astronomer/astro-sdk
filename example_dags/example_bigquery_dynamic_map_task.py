from datetime import datetime

from airflow import DAG
from airflow.decorators import task
from astro import sql as aql
from astro.sql import Table
from astro.sql.table import Metadata


@task
def summarize_campaign(capaign_id: str):
    print(capaign_id)


with DAG(
    dag_id="example_dynamic_map_task",
    schedule_interval=None,
    start_date=datetime(2022, 1, 1),
    catchup=False,
) as dag:
    bq_table = Table(
        name="active_campaigns",
        metadata=Metadata(
            schema="dag_authoring",
        ),
        conn_id="google_cloud_default",
    )

    @aql.transform()
    def get_campaigns(table: Table):
        return """select campaign_id from {{table}}"""


    summarize_campaign.expand(capaign_id=get_campaigns(table=bq_table))
