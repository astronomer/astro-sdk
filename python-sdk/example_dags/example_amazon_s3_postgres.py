import os
from datetime import datetime, timedelta

from airflow.models import DAG
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table
from pandas import DataFrame

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


@aql.dataframe(columns_names_capitalization="original")
def my_df_func(input_df: DataFrame):
    print(input_df)


with dag:
    my_homes_table = aql.load_file(
        input_file=File(path=f"{s3_bucket}/homes.csv"),
        # [START temp_table_example]  skipcq: PY-W0069
        output_table=Table(
            conn_id="postgres_conn",
        ),
        # [END temp_table_example]  skipcq: PY-W0069
    )
    sample_table = sample_create_table(my_homes_table)
    my_df_func(sample_table)
    # [START cleanup_example]  skipcq: PY-W0069
    aql.cleanup()
    # [END cleanup_example]  skipcq: PY-W0069
