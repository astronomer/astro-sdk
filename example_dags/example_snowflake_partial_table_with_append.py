import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag

from astro import dataframe
from astro.sql import append, load_file, run_raw_sql, transform, truncate
from astro.sql.tables import Metadata, Table

"""
Example ETL DAG highlighting Astro functionality
DAG requires 2 "Homes" csv's (found in this repo), and a supported database
General flow of the DAG is to extract the data from csv's and combine using SQL,
then switch to Python for a melt transformation, then back to SQL for final
filtering. The data is then loaded by appending to an existing reporting table.

This example DAG creates the reporting table & truncates it by the end of the execution.
"""

SNOWFLAKE_CONN_ID = "snowflake_conn"
dir_path = os.path.dirname(os.path.realpath(__file__))

FILE_PATH = dir_path + "/data/"


# The first transformation combines data from the two source csv's
@transform
def extract_data(homes1: Table, homes2: Table):
    return """
    SELECT *
    FROM {{homes1}}
    UNION
    SELECT *
    FROM {{homes2}}
    """


# Switch to Python (Pandas) for melting transformation to get data into long format
@dataframe
def transform_data(df: pd.DataFrame):
    df.columns = df.columns.str.lower()
    melted_df = df.melt(
        id_vars=["sell", "list"], value_vars=["living", "rooms", "beds", "baths", "age"]
    )

    return melted_df


# Back to SQL to filter data
@transform
def filter_data(homes_long: Table):
    return """
    SELECT *
    FROM {{homes_long}}
    WHERE SELL > 200
    """


@run_raw_sql
def create_table():
    """Create the reporting data which will be the target of the append method"""
    return """
    CREATE TABLE IF NOT EXISTS homes_reporting (
      sell number,
      list number,
      variable varchar,
      value number
    );
    """


@dag(start_date=datetime(2021, 12, 1), schedule_interval="@daily", catchup=False)
def example_snowflake_partial_table_with_append():

    # Initial load of homes data csv's into Snowflake
    homes_data1 = load_file(
        path=FILE_PATH + "homes.csv",
        output_table=Table(
            name="homes",
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            ),
        ),
    )

    homes_data2 = load_file(
        path=FILE_PATH + "homes2.csv",
        output_table=Table(
            name="homes2",
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            ),
        ),
    )

    # Define task dependencies
    extracted_data = extract_data(
        homes1=homes_data1,
        homes2=homes_data2,
        output_table=Table(name="combined_homes_data"),
    )

    transformed_data = transform_data(
        df=extracted_data, output_table=Table(name="homes_data_long")
    )

    filtered_data = filter_data(
        homes_long=transformed_data,
        output_table=Table(name="expensive_homes_long"),
    )

    create_results_table = create_table(conn_id=SNOWFLAKE_CONN_ID)

    # Append transformed & filtered data to reporting table
    # Dependency is inferred by passing the previous `filtered_data` task to `append_table` param
    record_results = append(
        append_table=filtered_data,
        columns=["sell", "list", "variable", "value"],
        main_table=Table(name="homes_reporting"),
    )
    record_results.set_upstream(create_results_table)

    # We truncate this table only to avoid wasting Snowflake resources
    # Why? Between 2022-03-25 and 2022-04-11 it accumulated 301G (89 million rows) because
    # this example DAG used to append rows without deleting them
    truncate_results = truncate(
        table=Table(
            name="homes_reporting",
            conn_id=SNOWFLAKE_CONN_ID,
            metadata=Metadata(
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
            ),
        )
    )
    truncate_results.set_upstream(record_results)


example_snowflake_partial_table_dag = example_snowflake_partial_table_with_append()
