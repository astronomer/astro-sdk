from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

import astronomer_sql_decorator.sql as aql
from astronomer_sql_decorator.sql.types import Table

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="pagila_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(
        minutes=30
    ),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    default_args=default_args,
)


@aql.transform(conn_id="postgres_conn", database="pagila")
def sample_pg(input_table: Table):
    return "SELECT * FROM {input_table} WHERE last_name LIKE 'G%%'"


with dag:
    last_name_g = sample_pg(input_table="actor")
