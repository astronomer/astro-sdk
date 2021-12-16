from datetime import datetime, timedelta

import pandas as pd
from airflow.decorators import dag

from astro import dataframe as df
from astro import sql as aql
from astro.sql.table import Table, TempTable


@aql.transform()
def combine_data(center_1: Table, center_2: Table):
    return """SELECT * FROM {center_1}
    UNION SELECT * FROM {center_2}"""


@aql.transform()
def clean_data(input_table: Table):
    return """SELECT * 
    FROM {input_table} WHERE TYPE NOT LIKE 'Guinea Pig'
    """


@df
def aggregate_data(df: pd.DataFrame):
    adoption_reporting_dataframe = df.pivot_table(
        index="SELL", values="LIST", columns=["TYPE"], aggfunc="count"
    ).reset_index()

    return adoption_reporting_dataframe


@dag(
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args={
        "email_on_failure": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
)
def animal_adoptions_etl():
    my_homes_table = aql.load_file(
        path="s3://tmp9/homes.csv",
        output_table=TempTable(
            database="pagila",
            conn_id="postgres_conn",
        ),
    )
    aggregated_data = aggregate_data(
        my_homes_table, output_table=Table("aggregated_home", conn_id="snowflake")
    )


animal_adoptions_etl_dag = animal_adoptions_etl()
