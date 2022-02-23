"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

from astro import sql as aql
from astro.dataframe import dataframe as df
from astro.sql.table import Table, TempTable

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="example_amazon_s3_postgres",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@aql.transform
def sample_create_table(input_table: Table):
    return "SELECT * FROM {{input_table}} LIMIT 10"


@df
def my_df_func(input_df: DataFrame):
    print(input_df)


with dag:
    my_homes_table = aql.load_file(
        path="s3://tmp9/homes.csv",
        output_table=TempTable(
            database="pagila",
            conn_id="postgres_conn",
        ),
    )
    sample_table = sample_create_table(my_homes_table)
    my_df_func(sample_table)
