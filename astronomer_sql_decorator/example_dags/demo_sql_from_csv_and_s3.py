from datetime import datetime
from airflow.decorators import dag
from airflow.utils import timezone


# Import Operator
from astronomer_sql_decorator.operators.postgres_decorator import postgres_decorator

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
def demo():
    @postgres_decorator(
        postgres_conn_id="postgres_conn", database="astro", from_s3=True
    )
    def task_from_s3(s3_path, input_table=None, output_table=None):
        return """SELECT "Sell" FROM %(input_table)s LIMIT 8"""

    @postgres_decorator(
        postgres_conn_id="postgres_conn", database="astro", from_csv=True
    )
    def task_from_local_csv(csv_path, input_table=None, output_table=None):
        return """SELECT "Sell" FROM %(input_table)s LIMIT 3"""

    t1 = task_from_s3(
        s3_path="s3://tmp9/homes.csv",
        input_table="input_raw_table_from_s3",
        output_table="expected_table_from_s3",
    )

    t2 = task_from_local_csv(
        csv_path="tests/data/homes.csv",
        input_table="input_raw_table_from_csv",
        output_table="expected_table_from_csv",
    )

    t1 >> t2


demo_dag = demo()
