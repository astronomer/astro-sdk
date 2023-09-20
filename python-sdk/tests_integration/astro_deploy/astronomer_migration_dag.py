"""Astronomer migration DAG to transform metadata from source deployment to target Astro Cloud deployment."""
from datetime import datetime

from airflow import DAG
from astronomer.starship.operators import AstroMigrationOperator

with DAG(
    dag_id="astronomer_migration_dag",
    start_date=datetime(2020, 8, 15),
    schedule_interval=None,
) as dag:
    AstroMigrationOperator(  # nosec B106
        task_id="export_meta",
        deployment_url='{{ dag_run.conf["deployment_url"] }}',
        token='{{ dag_run.conf["astro_token"] }}',
    )
