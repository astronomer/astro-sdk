import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag

from astro import dataframe
from astro.sql import append, load_file, transform
from astro.sql.table import Table

SNOWFLAKE_CONN_ID = "snowflake_conn"
dir_path = os.path.dirname(os.path.realpath(__file__))

FILE_PATH = dir_path + "/data/"


# Start by selecting data from two source tables in Snowflake
@transform
def extract_data(homes1: Table, homes2: Table):
    return """SELECT * FROM {{homes1}}
    UNION
    SELECT * FROM {{homes2}}
    """


# Switch to Pandas for melting transformation
@dataframe
def transform_data(df: pd.DataFrame):
    melted_df = df.melt(
        id_vars=["SELL", "LIST"], value_vars=["LIVING", "ROOMS", "BEDS", "BATHS", "AGE"]
    )
    return melted_df


@dag(start_date=datetime(2021, 12, 1), schedule_interval="@daily", catchup=False)
def example_snowflake_partial_table():
    # Load homes data
    homes_data1 = load_file(
        path=FILE_PATH + "homes.csv",
        output_table=Table(
            table_name="homes",
            conn_id=SNOWFLAKE_CONN_ID,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        ),
    )

    homes_data2 = load_file(
        path=FILE_PATH + "homes2.csv",
        output_table=Table(
            table_name="homes2",
            conn_id=SNOWFLAKE_CONN_ID,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
        ),
    )
    # Define task dependencies
    extracted_data = extract_data(
        homes1=homes_data1,
        homes2=homes_data2,
        output_table=Table(table_name="combined_homes_data"),
    )

    transformed_data = transform_data(
        extracted_data, output_table=Table("long_homes_data")
    )


example_snowflake_partial_table_dag = example_snowflake_partial_table()
