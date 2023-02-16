import os
import time
from datetime import datetime, timedelta

import pandas as pd

# Uses data from https://www.kaggle.com/c/shelter-animal-outcomes
from airflow.decorators import dag

from astro import sql as aql
from astro.files import File
from astro.table import Table


@aql.transform()
def combine_data(center_1: Table, center_2: Table):
    return """SELECT * FROM {{center_1}}
    UNION SELECT * FROM {{center_2}}"""


@aql.transform()
def clean_data(input_table: Table):
    return """SELECT *
    FROM {{input_table}} WHERE type NOT LIKE 'Guinea Pig'
    """


# Please note that this function will move a delta table into a local dataframe and not a spark dataframe.
# This is not recommended for large tables.
@aql.dataframe(columns_names_capitalization="original")
def aggregate_data(df: pd.DataFrame):
    new_df = df.pivot_table(index="date", values="name", columns=["type"], aggfunc="count").reset_index()
    new_df.columns = new_df.columns.str.lower()
    return new_df


@dag(
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval="@daily",
    default_args={
        "email_on_failure": False,
        "retries": 0,
        "retry_delay": timedelta(minutes=5),
    },
    catchup=False,
)
def example_amazon_s3_delta_transform():
    s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

    input_table_1 = Table(
        name="ADOPTION_CENTER_1_" + str(int(time.time())),
        conn_id="delta_conn",
        temp=True,
    )
    # [START metadata_example_delta]
    input_table_2 = Table(
        name="ADOPTION_CENTER_2_" + str(int(time.time())),
        conn_id="delta_conn",
        temp=True,
    )
    # [END metadata_example_delta]

    temp_table_1 = aql.load_file(
        input_file=File(path=f"{s3_bucket}/ADOPTION_CENTER_1_unquoted.csv"),
        output_table=input_table_1,
    )
    temp_table_2 = aql.load_file(
        input_file=File(path=f"{s3_bucket}/ADOPTION_CENTER_2_unquoted.csv"),
        output_table=input_table_2,
    )

    combined_data = combine_data(
        center_1=temp_table_1,
        center_2=temp_table_2,
    )

    cleaned_data = clean_data(combined_data)
    # [START dataframe_example_2]
    delta_output_table = Table(
        name="aggregated_adoptions_" + str(int(time.time())),
        conn_id="delta_conn",
        temp=True,
    )
    aggregate_data(
        cleaned_data,
        output_table=delta_output_table,
    )
    # [END dataframe_example_2]
    aql.cleanup()


dag = example_amazon_s3_delta_transform()
