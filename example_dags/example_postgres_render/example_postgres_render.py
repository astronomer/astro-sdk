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

import os
from datetime import datetime, timedelta

import pandas as pd
from airflow.models import DAG

from astro import sql as aql
from astro.dataframe import dataframe as adf

default_args = {
    "retries": 1,
    "retry_delay": 0,
}
dir_path = os.path.dirname(os.path.realpath(__file__))

dag = DAG(
    dag_id="example_postgres_render",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    template_searchpath=dir_path,
)


@adf
def print_results(df: pd.DataFrame):
    print(df.to_string())


with dag:
    models = aql.render(path="models")
    print_results(models["top_rentals"])
