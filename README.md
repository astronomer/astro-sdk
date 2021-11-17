<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [
  Airflow SQL Decorator
](#airflow-sql-decorator)
  - [Basic Usage](#basic-usage)
  - [Supported databases](#supported-databases)
  - [Loading Data](#loading-data)
  - [Transform](#transform)
  - [Raw SQL](#raw-sql)
  - [Appending data](#appending-data)
  - [Merging data](#merging-data)
  - [Truncate table](#truncate-table)
- [Dataframe functionality](#dataframe-functionality)
  - [from_sql](#from_sql)
  - [to_sql](#to_sql)

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
"""
Dependencies:
    xgboost
    scikit-learn
"""
from datetime import datetime, timedelta

import xgboost as xgb
from airflow.models import DAG
from pandas import DataFrame
from sklearn.metrics import accuracy_score
from sklearn.model_selection import train_test_split

from astronomer_sql_decorator import dataframe as adf
from astronomer_sql_decorator import sql as aql
from astronomer_sql_decorator.sql.types import Table

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}


dag = DAG(
    dag_id="pagila_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@aql.transform(conn_id="my_snowflake_conn", warehouse="LOADING")
def aggregate_orders(orders_table: Table):
    """Basic SELECT statement on a Snowflake table."""
    return """SELECT customer_id, count(*) AS purchase_count FROM {orders_table}
        WHERE purchase_date >= DATEADD(day, -7, '{{ execution_date }}')"""


@aql.transform(conn_id="postgres_conn")
def get_customers(customer_table: Table = Table("customer")):
    """Basic SELECT statement on a Postgres table."""
    return """SELECT customer_id, source, region, member_since
        FROM {customer_table} WHERE NOT is_deleted"""


@aql.transform(conn_id="postgres_conn")
def join_orders_and_customers(orders_table: Table, customer_table: Table):
    """Join the two tables to create a simple 'feature' dataset."""
    return """SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {orders_table} c LEFT OUTER JOIN {customer_table} p ON c.customer_id = p.customer_id"""


@aql.transform(conn_id="postgres_conn")
def get_existing_customers(customer_table: Table):
    """Filter for existing customers.

    Split the 'feature' dataset into 'current' and 'new' customers. This will be used for inference/scoring.
    """
    return """SELECT * FROM {customer_table} WHERE member_since > DATEADD(day, -7, '{{ execution_date }}')"""


@aql.transform(conn_id="postgres_conn")
def get_new_customers(customer_table: Table):
    """Filter for new customers."""
    return """SELECT * FROM {customer_table} WHERE member_since <= DATEADD(day, -7, '{{ execution_date }}')"""


@adf.from_sql(conn_id="postgres_conn")
def train_model(df: DataFrame):
    """Train model with Python.

    Convert upstream tables to Pandas DataFrames. Note how input database is inferred rather than explicitly stated in the decorator.
    """
    dfy = df.loc[:, "recent_purchase"]
    dfx = df.drop(columns=["customer_id", "recent_purchase"])
    dfx_train, dfx_test, dfy_train, dfy_test = train_test_split(
        dfx, dfy, test_size=0.2, random_state=63
    )
    model = xgb.XGBClassifier(
        n_estimators=100,
        eval_metric="logloss",
    )
    model.fit(dfx_train, dfy_train)
    preds = model.predict(dfx_test)
    print("Accuracy = {}".format(accuracy_score(dfy_test, preds)))
    return model


@adf.to_sql(output_table_name="final_table")
def score_model(model, df: DataFrame):
    """Score model.

    Note the model and input dataset as Task parameters.
    """
    preds = model.predict(df)
    output = df.copy()
    output["prediction"] = preds
    return output


SOURCE_TABLE = "source_finance_table"

s3_path = (
    f"s3://astronomer-galaxy-stage-dev/thanos/{SOURCE_TABLE}/"
    "{{ execution_date.year }}/"
    "{{ execution_date.month }}/"
    "{{ execution_date.day}}/"
    f"{SOURCE_TABLE}_"
    "{{ ts_nodash }}.csv"
)


with dag:
    """Structure DAG dependencies."""

    raw_orders = aql.load_file(
        path="input.csv", file_conn_id="my_s3_conn", output_conn_id="postgres_conn"
    )
    agg_orders = aggregate_orders(raw_orders)
    customers = get_customers()
    features = join_orders_and_customers(customers, agg_orders)
    existing = get_existing_customers(features)
    new = get_new_customers(features)
    model = train_model(existing)
    score_model(model=model, df=new)
```
## Supported databases

The current implementation supports Postgresql and Snowflake. Other databases are on the roadmap. 

To move data from one database to another, you can use the `save_file` and `load_file` functions to store intermediary tables on S3.

## Loading Data

To create an ELT pipeline, users can first load (CSV or parquet) data (from local, S3, or GCS) into a SQL database with the `load_sql` function. To interact with S3, set an S3 Airflow connection in the `AIRFLOW__SQL_DECORATOR__CONN_AWS_DEFAULT` environment variable.

```python
raw_orders = aql.load_file(
    path="s3://my/s3/path.csv",
    file_conn_id="my_s3_conn",
    output_conn_id="postgres_conn",
)
```

## Transform

With your data is in an SQL system, it's time to start transforming it! The `transform` function of
the SQL decorator is your "ELT" system. Each step of the transform pipeline creates a new table from the
`SELECT` statement and enables tasks to pass those tables as if they were native Python objects.

You will notice that the functions use a custom templating system. Wrapping a value in single brackets 
(like `{customer_table}`) indicates the value needs to be rendered as a SQL table. The SQL decorator
also treats values in double brackets as Airflow jinja templates. 

Please note that this is NOT an f string. F-strings in SQL formatting risk security breaches via SQL injections. 

For security, users MUST explicitly identify tables in the function parameters by typing a value as a `Table`. Only then will the SQL decorator treat the value as a table. 

```python
@aql.transform(conn_id="postgres_conn")
def get_orders():
    ...


@aql.transform(conn_id="postgres_conn")
def get_customers():
    ...


@aql.transform(conn_id="postgres_conn")
def join_orders_and_customers(orders_table: Table, customer_table: Table):
    """Join `orders_table` and `customers_table` to create a simple 'feature' dataset."""
    return """SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {orders_table} c LEFT OUTER JOIN {customer_table} p ON c.customer_id = p.customer_id"""


with dag:
    join_orders_and_customers(get_orders(), get_customers())
```

## Raw SQL
Most ETL use-cases can be addressed by cross-sharing Task outputs, as shown above with `@aql.transform`. For SQL operations that _don't_ return tables but might take tables as arguments, there is `@aql.run_raw_sql`. 

```python
@aql.run_raw_sql(conn_id="postgres_conn")
def drop_table(table_to_drop):
    return "DROP TABLE IF EXISTS {table_to_drop}"
```

## Appending data

Having transformed a table, you might want to append the results to a reporting table. An example of this might
be to aggregate daily data on a "main" table that analysts use for timeseries analysis. The `aql.append` function merges tables assuming that there are no conflicts. You can choose to merge the data 'as-is' or cast it to a new value if needed. Note that this query will fail if there is a merge conflict.

```python
foo = aql.append(
    conn_id="postgres_conn",
    database="postgres",
    append_table=APPEND_TABLE_NAME,
    columns=["Bedrooms", "Bathrooms"],
    casted_columns={"Age": "INTEGER"},
    main_table=MAIN_TABLE_NAME,
)
```

## Merging data

To merge data into an existing table in situations where there MIGHT be conflicts, the `aql.merge` function
adds data to a table with either an "update" or "ignore" strategy. The "ignore" strategy does not add values
that conflict, while the "update" strategy overwrites the older values. This function only handles basic merge statements. Use the `run_raw_sql` function for complex statements. 

Note that the `merge_keys` parameter is a list in Postgres, but a map in Snowflake. This syntax decision was unavoidable due to the differences in how Postgres and Snowflake handle conflict resolution. Also note that `*` inserts are disabled for the merge function.

Postgres:
```python
a = aql.merge(
    target_table="merge_test_1",
    merge_table="merge_test_2",
    merge_keys=["list", "sell"],
    target_columns=["list", "sell", "taxes"],
    merge_columns=["list", "sell", "age"],
    conn_id="postgres_conn",
    conflict_strategy="update",
    database="pagila",
)
```
Snowflake:
```python
a = aql.merge(
    target_table="merge_test_1",
    merge_table="merge_test_2",
    merge_keys={"list": "list", "sell": "sell"},
    target_columns=["list", "sell"],
    merge_columns=["list", "sell"],
    conn_id="snowflake_conn",
    database="DWH_LEGACY",
    conflict_strategy="ignore",
)
```

## Truncate table

```python
a = aql.truncate(
    table="truncate_table",
    conn_id="snowflake_conn",
    database="DWH_LEGACY",
)
```

# Dataframe functionality

Finally, your pipeline might call for procedures that would be too complex or impossible in SQL. This could be building a model from a feature set, or using a windowing function which more Pandas is adept for. The `from_sql` and `to_sql` functions can easily move your data into a Pandas dataframe and back to your database as needed.

The `from_sql` function requires a parameter named "df". This parameter will be the table that you wish to process. At runtime, the operator loads and acts on the table as a Pandas DataFrame. If the Task returns a DataFame, downstream Taskflow API Tasks can interact with it to continue using Python.

After transforming data with Python, the `to_sql` decorator can convert DataFrames SQL. This decorator MUST return a DataFrame which will be transformed into a temporary table on the database specified by the `conn_id` (or a permanent table if you declare a table name). 


## from_sql
```python
@adf.from_sql(conn_id="postgres_conn", database="pagila")
def my_df_func(df=None):
    return df.actor_id.count()
```


## to_sql
```python
import pandas as pd


@adf.to_sql(conn_id="postgres_conn", database="pagila", output_table_name="foo")
def my_df_func():
    return pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
```