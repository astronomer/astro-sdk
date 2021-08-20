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
from astronomer_sql_decorator.operators.sql_decorator import postgres_decorator
from airflow.models import DAG
from pandas import DataFrame
from datetime import datetime, timedelta

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
}

dag = DAG(dag_id="pagila_dag",
          start_date=datetime(2019, 1, 1),
          max_active_runs=3,
          schedule_interval=timedelta(minutes=30),  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
          default_args=default_args,
          )

@postgres_decorator(postgres_conn_id="my_favorite_db", database="pagila")
def sample_pg(input_table):
    return "SELECT * FROM %(input_table)s WHERE last_name LIKE 'G%%'"



@postgres_decorator(postgres_conn_id="my_favorite_db", database="pagila", to_dataframe=True)
def print_table(input_df: DataFrame):
    print(input_df.to_string)


with dag:
    last_name_g = sample_pg(input_table="actor")
    print_table(last_name_g)

```