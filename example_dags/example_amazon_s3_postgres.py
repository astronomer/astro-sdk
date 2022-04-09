import os
from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

from astro import sql as aql
from astro.dataframe import dataframe as df
from astro.sql.table import Table, TempTable

s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_amazon_s3_postgres",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@aql.transform
def sample_create_table(input_table: Table):
    return "SELECT * FROM {{input_table}} LIMIT 10"


@df(identifiers_as_lower=False)
def my_df_func(input_df: DataFrame):
    print(input_df)


with dag:
    my_homes_table = aql.load_file(
        path=f"{s3_bucket}/homes.csv",
        output_table=TempTable(
            database="pagila",
            conn_id="postgres_conn",
        ),
    )
    sample_table = sample_create_table(my_homes_table)
    my_df_func(sample_table)
