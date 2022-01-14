from datetime import datetime, timedelta

from airflow.models import DAG

from astro import sql as aql
from astro.sql.parsers.sql_directory_parser import ParsedSqlOperator
from astro.sql.table import Table

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="sql_file_explicit",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)
import os

dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    """Structure DAG dependencies.
    So easy! It's like magic!
    """
    #
    # raw_orders = aql.load_file(
    #     path="s3://my/path/foo.csv",
    #     file_conn_id="my_s3_conn",
    #     output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
    # )
    a = ParsedSqlOperator(file_name="agg_orders.sql", sql="foo", parameters={})
    p = {"agg_orders": a.output}
    b = ParsedSqlOperator(
        file_name="join_customers_and_orders.sql", sql="foo", parameters={}
    )
    b.parameters = p
