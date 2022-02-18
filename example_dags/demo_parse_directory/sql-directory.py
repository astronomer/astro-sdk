import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.models import DAG

from astro import sql as aql
from astro.dataframe import dataframe as adf

default_args = {
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="sql_file_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@adf
def print_results(df: pd.DataFrame):
    print(df.to_string)


dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    models = aql.render(dir_path + "/ingest_models")
    print_results(models["inherit_task"])
