<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [
  Airflow SQL Decorator
](#airflow-sql-decorator)
  - [Basic Usage](#basic-usage)
  - [Supported databases](#supported-databases)
  - [The Table class](#the-table-class)
  - [Loading Data](#loading-data)
  - [Transform](#transform)
  - [Raw SQL](#raw-sql)
  - [Appending data](#appending-data)
  - [Merging data](#merging-data)
  - [Truncate table](#truncate-table)
- [Dataframe functionality](#dataframe-functionality)
  - [dataframe](#dataframe)
  - [ML Operations](#ml-operations)
  - [train](#train)
  - [predict](#predict)

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

from astro import sql as aql
from astro.ml import predict, train
from astro.sql.table import Table

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


@aql.transform
def aggregate_orders(orders_table: Table):
    """Snowflake.
    Next I would probably do some sort of merge, but I'll skip that for now. Instead, some basic ETL.
    Note the Snowflake-specific parameter...
    Note that I'm not specifying schema location anywhere. Ideally this can be an admin setting that
    I'm able to over-ride.
    """
    return """SELECT customer_id, count(*) AS purchase_count FROM {orders_table}
        WHERE purchase_date >= DATEADD(day, -7, '{{ execution_date }}')"""


@aql.transform(conn_id="postgres_conn", database="pagila")
def get_customers(customer_table: Table = Table("customer")):
    """Basic clean-up of an existing table."""
    return """SELECT customer_id, source, region, member_since
        FROM {customer_table} WHERE NOT is_deleted"""


@aql.transform
def join_orders_and_customers(orders_table: Table, customer_table: Table):
    """Now join those together to create a very simple 'feature' dataset."""
    return """SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {orders_table} c LEFT OUTER JOIN {customer_table} p ON c.customer_id = p.customer_id"""


@aql.transform
def get_existing_customers(customer_table: Table):
    """Filter for existing customers.
    Split this 'feature' dataset into existing/older customers and 'new' customers, which we'll use
    later for inference/scoring.
    """
    return """SELECT * FROM {customer_table} WHERE member_since > DATEADD(day, -7, '{{ execution_date }}')"""


@aql.transform
def get_new_customers(customer_table: Table):
    """Filter for new customers.
    Split this 'feature' dataset into existing/older customers and 'new' customers, which we'll use
    later for inference/scoring.
    """
    return """SELECT * FROM {customer_table} WHERE member_since <= DATEADD(day, -7, '{{ execution_date }}')"""


@train()
def train_model(df: DataFrame):
    """Train model with Python.
    Switch to Python. Note that I'm not specifying the database input in the decorator. Ideally,
    the decorator knows where the input is coming from and knows that it needs to convert the
    table to a pandas dataframe. Then I can use the same task for a different database or another
    type of input entirely. Less for the user to specify, easier to reuse for different inputs.
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


@predict()
def score_model(model, df: DataFrame):
    """In this task I'm passing in the model as well as the input dataset."""
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
    """Structure DAG dependencies.
    So easy! It's like magic!
    """

    raw_orders = aql.load_file(
        path="to-do",
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
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

## The Table class

To instantiate a table or bring in a table from a database into the `astro` ecosystem, you can pass a `Table` object into the class. This Table object will contain all necessary metadata
to handle table creation between tasks. once you define it in the beginning of your pipeline, `astro` can automatically pass that metadata along

```python
from astro import sql as aql
from astro.sql.table import Table


@aql.transform
def my_first_sql_transformation(input_table: Table):
    return "SELECT * FROM {input_table}"


@aql.transform
def my_second_sql_transformation(input_table_2: Table):
    return "SELECT * FROM {input_table_2}"


with dag:
    my_table = my_first_sql_transformation(
        input_table=Table(table_name="foo", database="bar", conn_id="postgres_conn")
    )
    my_second_sql_transformation(my_table)
```

## Loading Data

To create an ELT pipeline, users can first load (CSV or parquet) data (from local, S3, or GCS) into a SQL database with the `load_sql` function. 
To interact with S3, set an S3 Airflow connection in the `AIRFLOW__SQL_DECORATOR__CONN_AWS_DEFAULT` environment variable.

```python
from astro import sql as aql
from astro.sql.table import Table

raw_orders = aql.load_file(
    path="s3://my/s3/path.csv",
    file_conn_id="my_s3_conn",
    output_table=Table(table_name="my_table", conn_id="postgres_conn"),
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
@aql.transform
def get_orders():
    ...


@aql.transform
def get_customers():
    ...


@aql.transform
def join_orders_and_customers(orders_table: Table, customer_table: Table):
    """Join `orders_table` and `customers_table` to create a simple 'feature' dataset."""
    return """SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {orders_table} c LEFT OUTER JOIN {customer_table} p ON c.customer_id = p.customer_id"""


with dag:
    orders = get_orders()
    customers = get_customers()
    join_orders_and_customers(orders, customers)
```

## Raw SQL
Most ETL use-cases can be addressed by cross-sharing Task outputs, as shown above with `@aql.transform`. For SQL operations that _don't_ return tables but might take tables as arguments, there is `@aql.run_raw_sql`. 

```python
@aql.run_raw_sql
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
    append_table=APPEND_TABLE,
    columns=["Bedrooms", "Bathrooms"],
    casted_columns={"Age": "INTEGER"},
    main_table=MAIN_TABLE,
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
    target_table=MAIN_TABLE,
    merge_table=MERGE_TABLE,
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
    target_table=MAIN_TABLE,
    merge_table=MERGE_TABLE,
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
    table=TRUNCATE_TABLE,
    conn_id="snowflake_conn",
    database="DWH_LEGACY",
)
```

# Dataframe functionality

Finally, your pipeline might call for procedures that would be too complex or impossible in SQL. This could be building a model from a feature set, or using a windowing function which more Pandas is adept for. The `df` functions can easily move your data into a Pandas dataframe and back to your database as needed.

At runtime, the operator loads any `Table` object into a Pandas DataFrame. If the Task returns a DataFame, downstream Taskflow API Tasks can interact with it to continue using Python.

If after running the function, you wish to return the value into your database, simply include a `Table` in the reserved `output_table` parameters (please note that since this parameter is reserved, you can not use it in your function definition).



## dataframe
```python
from astro import dataframe as df
from astro import sql as aql
from astro.sql.table import Table
import pandas as pd


@df
def get_dataframe():
    return pd.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})


@aql.transform
def sample_pg(input_table: Table):
    return "SELECT * FROM {input_table}"


with self.dag:
    my_df = get_dataframe(
        output_table=Table(
            table_name="my_df_table", conn_id="postgres_conn", database="pagila"
        )
    )
    pg_df = sample_pg(my_df)
```

## ML Operations

We currently offer two ML based functions: `train` and `predict`. Currently these functions do the 
exact same thing as `dataframe`, but eventually we hope to add valuable ML functionality (e.g. hyperparam for train and
model serving options in predict).

For now please feel free to use these endpoints as convenience functions, knowing that there will long term be added
functionality.

## train
```python
from astro.ml import train


@train
def my_df_func():
    return pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
```

## predict
```python
from astro.ml import predict


@predict
def my_df_func():
    return pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})
```