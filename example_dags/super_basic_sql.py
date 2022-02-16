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

import astro.sql as aql
from astro.sql.table import Table

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
}

dag = DAG(
    dag_id="pagila_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(
        minutes=30
    ),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
    default_args=default_args,
)


@aql.transform(conn_id="postgres_conn", database="pagila")
def sample_pg(input_table: Table):
    return "SELECT * FROM {{input_table}} WHERE last_name LIKE 'G%%'"


with dag:
    last_name_g = sample_pg(input_table=Table(table_name="actor"))
