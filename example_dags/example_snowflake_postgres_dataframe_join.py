import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag

from astro import dataframe as df
from astro.sql import load_file
from astro.sql.table import Table

SNOWFLAKE_CONN_ID = "snowflake_conn"
POSTGRES_CONN_ID = "postgres_conn"
dir_path = os.path.dirname(os.path.realpath(__file__))

FILE_PATH = dir_path + "/data/"


# The first transformation combines data from the two source csv's
@df(conn_id="postgres_conn")
def join_data(homes1: pd.DataFrame, homes2: pd.DataFrame):
    df = homes1.join(homes2)
    print(df)
    return df


@dag(start_date=datetime(2021, 12, 1), schedule_interval="@daily", catchup=False)
def example_snowflake_postgres_dataframe_join():

    # Initial load of homes data csv's into Snowflake
    homes_data1 = load_file(
        path=FILE_PATH + "homes.csv",
        output_table=Table(
            table_name="homes",
            conn_id=SNOWFLAKE_CONN_ID,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        ),
    )

    homes_data2 = load_file(
        path=FILE_PATH + "homes2.csv",
        output_table=Table(table_name="homes2", conn_id=POSTGRES_CONN_ID),
    )

    # Define task dependencies
    join_data(
        homes1=homes_data1,
        homes2=homes_data2,
        output_table=Table(table_name="combined_homes_data", conn_id=POSTGRES_CONN_ID),
    )


example_snowflake_postgres_dataframe_join = example_snowflake_postgres_dataframe_join()
