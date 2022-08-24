import os
import time
from datetime import datetime, timedelta

import pandas as pd

# Uses data from https://www.kaggle.com/c/shelter-animal-outcomes
from airflow.models import DAG

from astro import sql as aql
from astro.files import File
from astro.sql.table import Metadata, Table

dag = DAG(
    dag_id="my_webinar_dag",
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


@aql.transform
def filter_duplicate_posts(post: Table):
    return "SELECT DISTINCT * FROM {{post}}"


@aql.transform()
def combine_data(center_1: Table, center_2: Table):
    return """SELECT * FROM {{center_1}}
    UNION SELECT * FROM {{center_2}}"""


s3_bucket = os.getenv("S3_BUCKET", "s3://tmp9")

with dag:
    temp_table_1 = aql.load_file(...)
    temp_table_2 = aql.load_file(...)
    aql.transform_file(
        file_path="/path/to/combine_data.sql",
        parameters={"center_1": temp_table_1, "center_2": temp_table_2},
    )

    aql.cleanup()


@aql.transform
def filter_duplicate_posts(post: Table):
    return "SELECT DISTINCT * FROM {{post}}"


@aql.transform()
def combine_data(table_1: Table, table_2: Table):
    return """SELECT * FROM {{table_1}}
    UNION SELECT * FROM {{table_2}}"""


with dag:
    raw_reddit_2019 = aql.load_file(...)
    raw_reddit_2020 = aql.load_file(...)
    fi
    combine_data(
        filter_duplicate_posts(raw_reddit_2019), filter_duplicate_posts(raw_reddit_2020)
    )

    aql.cleanup()


@aql.transform
def filter_null_posts(input_table: Table):
    return "SELECT * FROM {{input_table}} WHERE body IS NOT null)"


@aql.transform
def add_features_to_posts(filtered_posts: Table, feature_table: Table):
    return """SELECT ft.post_id, user_name, fp.upvotes, FROM {{filtered_posts}} fp
                  JOIN {{feature_table}} ft ON fp.post_id = ft.post_id"""


feature_table = Table(name=REDDIT_FEATURES, conn_id=SNOWFLAKE_CONN_ID)
filtered_data = filter_null_posts(orders_data)
joined_data = add_features_to_posts(
    filtered_data,
    feature_table,
    output_table=Table(
        name="data_with_features_{{ run_id }}", conn_id="snowflake_conn"
    ),
)
