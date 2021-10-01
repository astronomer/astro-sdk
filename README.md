<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [
  Airflow SQL Decorator
](#airflow-sql-decorator)
  - [Basic Usage](#basic-usage)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
</p>
<h1 align="center">
  Airflow SQL Decorator
</h1>
  <h3 align="center">
  Your new Airflow + SQL experience. Maintained with ❤️ by Astronomer.
</h3>
<br/>

## Basic Usage

```python
import astronomer_sql_decorator.sql as aql
from airflow.models import DAG
from pandas import DataFrame
from datetime import datetime, timedelta
from astronomer_sql_decorator.sql.types import Table

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


@aql.transform(
    postgres_conn_id="postgres_conn",
    database="pagila",
    output_table="my_raw_data",
    from_s3=True,
)
def task_from_s3(s3_path, input_table: Table = None, output_table=None):
    return """SELECT * FROM %(input_table)s"""


@aql.transform(postgres_conn_id="postgres_conn", database="pagila", from_csv=True)
def task_from_local_csv(csv_path, input_table: Table = None, output_table=None):
    return """SELECT "Sell" FROM %(input_table)s LIMIT 3"""


@aql.transform(postgres_conn_id="my_favorite_db", database="pagila")
def sample_pg(input_table: Table, last_name_prefix):
    return (
        "SELECT * FROM %(input_table)s WHERE last_name LIKE '$(last_name_prefix)s%%'",
        {"last_name_prefix": last_name_prefix},
    )


@aql.transform(postgres_conn_id="my_favorite_db", database="pagila", to_dataframe=True)
def print_table(input_df: DataFrame):
    print(input_df.to_string)


with dag:
    import_pagila_table = task_from_s3(
        s3_path="s3://tmp9/homes.csv", input_table="input_raw_table_from_s3"
    )
    last_name_g = sample_pg(input_table=import_pagila_table, last_name_prefix="F")
    print_table(last_name_g)
```