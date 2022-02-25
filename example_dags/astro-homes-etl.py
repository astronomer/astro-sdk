from airflow.decorators import dag
from astro.sql import transform, append, load_file
from astro.sql.table import Table
from astro import dataframe

from datetime import datetime
import pandas as pd

"""
Example ETL DAG highlighting Astro functionality
DAG requires 2 "Homes" csv's (found in this repo), and a supported database

General flow of the DAG is to extract the data from csv's and combine using SQL,
then switch to Python for a melt transformation, then back to SQL for final
filtering. The data is then loaded by appending to an existing reporting table.
"""
# Airflow connection to the database (Snowflake in this case)
SNOWFLAKE_CONN_ID = "snowflake_conn"
# Path to Homes csv's
FILE_PATH = '../tests/data/'

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
    melted_df = df.melt(id_vars=['sell', 'list'], 
                        value_vars=['living', 'rooms', 'beds', 'baths', 'age'])

    return melted_df

# Back to SQL to filter data
@transform
def filter_data(homes_long: Table):
    return """
    SELECT *
    FROM {{homes_long}}
    WHERE SELL > 200
    """


@dag(start_date=datetime(2022, 2, 1), schedule_interval='@daily', catchup=False)

def aastroflow_homes_etl_dag():
    # Initial load of homes data csv's into Snowflake
    homes_data1 = load_file(
        path=FILE_PATH + 'homes.csv',
        output_table=Table(table_name="homes", conn_id=SNOWFLAKE_CONN_ID, database="SANDBOX"),
    )

    homes_data2 = load_file(
        path=FILE_PATH + 'homes2.csv',
        output_table=Table(table_name="homes2", conn_id=SNOWFLAKE_CONN_ID, database="SANDBOX", schema="KENTENDANAS"),
    )
    # Define task dependencies
    extracted_data = extract_data(
        homes1=homes_data1,
        homes2=homes_data2,
        output_table=Table(table_name="combined_homes_data")
    )

    transformed_data = transform_data(
        df=extracted_data,
        output_table=Table('homes_data_long')
    )

    filtered_data = filter_data(
        homes_long=transformed_data,
        output_table=Table(table_name="expensive_homes_long")
    )
    
    # Append transformed & filtered data to reporting table
    # Dependency is inferred by passing the previous `filtered_data` task to `append_table` param
    append(
        append_table=filtered_data,
        columns=["SELL", "LIST", "VARIABLE", "VALUE"],
        main_table=Table(table_name="homes_reporting"),
    )

astroflow_hoames_etl_dag = aastroflow_homes_etl_dag()
