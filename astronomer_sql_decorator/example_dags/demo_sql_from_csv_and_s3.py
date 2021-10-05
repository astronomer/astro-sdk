from datetime import datetime

from airflow.decorators import dag
from airflow.utils import timezone

# Import Operator
import astronomer_sql_decorator.sql as aql

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


@dag(
    default_args=default_args,
    schedule_interval=None,
    start_date=timezone.utcnow(),
    tags=["demo"],
)
def demo_with_s3_and_csv():
    t1 = aql.load_file(
        path="s3://tmp9/homes.csv",
        file_conn_id="my_aws_conn",
        output_conn_id="postgres_conn",
        database="astro",
        output_table_name="expected_table_from_s3",
    )

    t2 = aql.load_file(
        path="tests/data/homes.csv",
        output_conn_id="postgres_conn",
        database="astro",
        output_table_name="expected_table_from_csv",
    )

    aql.save_file(
        output_file_path="s3://tmp9/homes.csv",
        table=t1,
        input_conn_id="postgres_conn",
        overwrite=True,
    )

    aql.save_file(
        output_file_path="tests/data/homes_output.csv",
        table=t2,
        input_conn_id="postgres_conn",
        overwrite=True,
    )


demo_dag = demo_with_s3_and_csv()
