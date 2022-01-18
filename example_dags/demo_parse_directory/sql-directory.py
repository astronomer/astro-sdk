import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.models import DAG

import astro.dataframe as df
from astro import sql as aql
from astro.sql.table import Table

default_args = {
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="sql_file_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@df
def aggregate_data(df: pd.DataFrame):
    customers_and_orders_dataframe = df.pivot_table(
        index="DATE", values="NAME", columns=["TYPE"], aggfunc="count"
    ).reset_index()
    return customers_and_orders_dataframe


dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    """Structure DAG dependencies.
    So easy! It's like magic!
    """
    #
    raw_orders = aql.load_file(
        path="s3://my/path/{{ execution_date }}/",
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
    )
    ingest_models = aql.render_directory(
        dir_path + "/ingest_models", orders_table=raw_orders
    )
    transform_models = aql.render_directory(
        dir_path + "/transform_models",
        orders_and_customer_table=ingest_models["orders_and_customer_table"],
    )

    aggregate_data(df=ingest_models["join_customers_and_orders"])
