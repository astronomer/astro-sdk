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
    start_date=datetime(2021, 1, 1),
    catchup=False,
) as dag:
    input_table_2 = Table(
        name="dummy_table",
        metadata=Metadata(
            schema="pankaj_upsert_dataset",
        ),
        conn_id="google_cloud_default",
    )

    @aql.run_raw_sql(conn_id="google_cloud_default")
    def get_campaigns(table: Table):
        return """select * from {{table}}"""


    summarize_campaign.expand(capaign_id=get_campaigns(table=input_table_2))
