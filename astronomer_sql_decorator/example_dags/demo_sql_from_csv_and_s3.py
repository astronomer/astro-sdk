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


@aql.transform(conn_id="postgres_conn", database="astro", to_s3=True)
def task_to_s3(s3_path, input_table=None):
    return """SELECT "Sell" FROM %(input_table)s"""


@aql.transform(conn_id="postgres_conn", database="astro", to_csv=True)
def task_to_local_csv(csv_path, input_table=None):
    return """SELECT "Sell" FROM %(input_table)s"""


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

    task_to_s3(s3_path="s3://tmp9/homes.csv", input_table=t1)

    task_to_local_csv(csv_path="tests/data/homes_output.csv", input_table=t2)

    t1 >> t2


demo_dag = demo_with_s3_and_csv()
