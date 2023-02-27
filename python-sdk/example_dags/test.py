import os
from datetime import datetime

import pandas as pd
from airflow.decorators import dag

from astro.files import File
from astro.sql import append, cleanup, dataframe, load_file, run_raw_sql, transform
from astro.sql import DataframeOperator
from astro.table import Metadata, Table

SNOWFLAKE_CONN_ID = "snowflake_conn"
dir_path = os.path.dirname(os.path.realpath(__file__))

FILE_PATH = dir_path + "/data/"
from astro.files import get_file_list
from airflow.decorators import task


@task
def list_files():
    return get_file_list(path=f"s3://astro-sdk/homes", conn_id="minio_conn")


@dataframe()
def dummy(input_file):
    print(input_file)


@dag(dag_id="test", start_date=datetime(2021, 12, 1), schedule_interval=None, catchup=False)
def example_snowflake_partial_table_with_append():
    dummy.expand(input_file=get_file_list(path=f"s3://astro-sdk/homes", conn_id="minio_conn"))
    cleanup()

