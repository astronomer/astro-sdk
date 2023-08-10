import os
import time
from datetime import datetime, timedelta

import pandas as pd

# Uses data from https://www.kaggle.com/c/shelter-animal-outcomes
from airflow.decorators import dag

from astro import sql as aql
from astro.files import File
from astro.options import SnowflakeLoadOptions
from astro.query_modifier import QueryModifier
from astro.table import Metadata, Table


@aql.transform(assume_schema_exists=True)
def combine_data(center_1: Table, center_2: Table):
    return """SELECT * FROM {{center_1}}
    UNION SELECT * FROM {{center_2}}"""


@aql.transform(
    assume_schema_exists=True,
    query_modifier=QueryModifier(pre_queries=["ALTER SESSION SET query_tag='not_guinea_pig';"]),
)
def clean_data(input_table: Table):
    return """SELECT *
    FROM {{input_table}} WHERE type NOT LIKE 'Guinea Pig'
    """


# [START dataframe_example_1]
@aql.dataframe(columns_names_capitalization="original")
def aggregate_data(df: pd.DataFrame):
    new_df = df.pivot_table(index="date", values="name", columns=["type"], aggfunc="count").reset_index()
    new_df.columns = new_df.columns.str.lower()
    return new_df


# [END dataframe_example_1]


@dag(
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval=None,
    default_args={
        "email_on_failure": False,
        "retries": 1,
        "retry_delay": timedelta(seconds=5),
    },
    catchup=False,
)
def example_amazon_s3_snowflake_transform():
    s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

    input_table_1 = Table(
        name="ADOPTION_CENTER_1_" + str(int(time.time())),
        metadata=Metadata(
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
        ),
        conn_id="snowflake_conn",
        temp=True,
    )
    # [START metadata_example_snowflake]
    input_table_2 = Table(
        name="ADOPTION_CENTER_2_" + str(int(time.time())),
        metadata=Metadata(
            database=os.environ["SNOWFLAKE_DATABASE"],
            schema=os.environ["SNOWFLAKE_SCHEMA"],
        ),
        conn_id="snowflake_conn",
        temp=True,
    )
    # [END metadata_example_snowflake]

    temp_table_1 = aql.load_file(
        input_file=File(path=f"{s3_bucket}/ADOPTION_CENTER_1_unquoted.csv"),
        output_table=input_table_1,
        load_options=[
            SnowflakeLoadOptions(
                file_options={"TYPE": "CSV", "TRIM_SPACE": True},
                copy_options={"ON_ERROR": "CONTINUE"},
            )
        ],
        use_native_support=False,
    )
    temp_table_2 = aql.load_file(
        input_file=File(path=f"{s3_bucket}/ADOPTION_CENTER_2_unquoted.csv"),
        output_table=input_table_2,
        load_options=[
            SnowflakeLoadOptions(
                file_options={"TYPE": "CSV", "TRIM_SPACE": True},
                copy_options={"ON_ERROR": "CONTINUE"},
            )
        ],
        use_native_support=False,
    )

    combined_data = combine_data(
        center_1=temp_table_1,
        center_2=temp_table_2,
    )

    cleaned_data = clean_data(combined_data)
    # [START dataframe_example_2]
    snowflake_output_table = Table(
        name="aggregated_adoptions_" + str(int(time.time())),
        metadata=Metadata(
            schema=os.environ["SNOWFLAKE_SCHEMA"],
            database=os.environ["SNOWFLAKE_DATABASE"],
        ),
        conn_id="snowflake_conn",
        temp=True,
    )
    aggregate_data(
        cleaned_data,
        output_table=snowflake_output_table,
    )
    # [END dataframe_example_2]
    aql.cleanup()


dag = example_amazon_s3_snowflake_transform()
