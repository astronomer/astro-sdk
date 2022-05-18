import os

from airflow.decorators import dag
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.sql.table import Table

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}
s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=timezone.utcnow(),
    tags=["demo"],
)
def example_amazon_s3_postgres_load_and_save():
    t1 = aql.load_file(
        path=f"{s3_bucket}/homes.csv",
        file_conn_id="",
        output_table=Table(name="expected_table_from_s3", conn_id="postgres_conn"),
    )

    aql.save_file(
        input_data=t1,
        output_file_path=f"{s3_bucket}/homes.csv",
        overwrite=True,
    )


example_amazon_s3_postgres_load_and_save_dag = (
    example_amazon_s3_postgres_load_and_save()
)
