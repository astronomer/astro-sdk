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
@aql.transform(postgres_conn_id="my_favorite_db", database="pagila")
def sample_pg(actor: Table, last_name_prefix):
    return (
        "SELECT * FROM {actor} WHERE last_name LIKE '{last_name_prefix}s%%'",
        {"last_name_prefix": last_name_prefix},
    )


with dag:
    import_pagila_table = aql.load_file(
        path="s3://tmp9/homes.csv",
        output_conn_id="my_favorite_db",
        output_table_name="foo",
    )
    last_name_g = sample_pg(input_table=import_pagila_table, last_name_prefix="F")
```