import os
from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

from astro import sql as aql
from astro.files import File
from astro.sql.table import MetaData, Table

s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")
gcs_bucket = os.getenv("GCS_BUCKET", "gcs://tmp9")

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
def get_recent_customer(
    input_table: Table, output_table=Table(name="new_customer_data")
):
    return "SELECT * FROM {{input_table}} where onboardDate > 2020-1-1"


@aql.dataframe(columns_names_capitalization="original")
def my_df_func(input_df: DataFrame):
    print(input_df)


with dag:
    # load
    customer_data = aql.load_file(
        input_file=File(path=f"{s3_bucket}/data.csv"),
        output_table=Table(conn_id="bigquery_conn", metadata=MetaData(schema="schema")),
    )
    # transform
    new_customer_table = get_recent_customer(customer_data)
    # export
    aql.export_file(
        input_data=new_customer_table, output_file=File(path=f"{gcs_bucket}/data.csv")
    )
    aql.cleanup()
