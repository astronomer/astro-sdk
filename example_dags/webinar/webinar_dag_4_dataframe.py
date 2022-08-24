import os
import time
from datetime import datetime, timedelta

import pandas as pd

# Uses data from https://www.kaggle.com/c/shelter-animal-outcomes
from airflow.models import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.table import Metadata, Table

dag = DAG(
    dag_id="my_webinar_dag",
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    default_args={
        "email_on_failure": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
)


def run_against_model(df: pd.DataFrame):
    return df


@aql.dataframe()
def run_against_model(df: pd.DataFrame):
    new_df = run_against_model(df)
    return new_df


s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

with dag:
    temp_table_1 = aql.load_file(...)
    temp_table_2 = aql.load_file(...)
    combined_data = aql.transform_file(
        file_path="/path/to/combine_data.sql",
        parameters={"center_1": temp_table_1, "center_2": temp_table_2},
    )
    run_against_model(combined_data, output_table=Table(conn_id="snowflake_conn"))

    aql.cleanup()
