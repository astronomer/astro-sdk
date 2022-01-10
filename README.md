<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [
  Astro :rocket:
](#astro-rocket)
  - [Philosophy](#philosophy)
  - [Setup](#setup)
  - [Basic Usage](#basic-usage)
  - [Supported databases](#supported-databases)
  - [The output_table parameter](#the-output_table-parameter)
  - [Schemas](#schemas)
  - [Loading Data](#loading-data)
  - [Transform](#transform)
  - [Transform File](#transform-file)
  - [Raw SQL](#raw-sql)
- [Other SQL functions](#other-sql-functions)
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
  Astro :rocket:
</h1>
  <h3 align="center">
  Your new Airflow DAG writing experience. Maintained with ❤️ by Astronomer.
</h3>
<br/>

## Philosophy

With the `astro` library, we want to redefine the DAG writing experience from the bottom up. Our goal is to empower
data engineers and data scientists to write DAGs based around the momevent of _data_ instead of the dependencies of tasks.
With this in mind, we built a library where every step is defined by how your data moves, while also simplifying the transformation
process between different environments. Our first two integrations are SQL and pandas, but we are planning many more in coming months.

With our SQL and dataframe modules, you should have the ability to treat SQL tables as if they're python objects. You can manipulate them,
join them, templatize them, and ultimately turn them into dataframes if you want to run python functions against them. We hope that this
library creates a cleaner Airflow ELT experience, as well as an easier onboarding for those who want to think in data transformations
instead of DAGs. 

Please feel free to raise issues and propose improvements, and community contributions are highly welcome!

Thank you,

:sparkles: The Astro Team :sparkles:

## Setup

To install the astro library simply run
```shell script
pip install astro-projects
```

or add `astro-projects` to your `requirements.txt`.

Before running please keep in mind that you'll need to set the following env variable for passing table objects
between tasks:

```shell script
AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
```

## Basic Usage

```python
from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

from astro import sql as aql
from astro import dataframe as df
from astro.sql.table import Table

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="astro_example_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


@aql.transform
def aggregate_orders(orders_table: Table):
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


@df
def perform_dataframe_transformation(df: DataFrame):
    """Train model with Python. You can import any python library you like and treat this as you would a normal
    dataframe
    """
    recent_purchases_dataframe = df.loc[:, "recent_purchase"]
    return recent_purchases_dataframe


@df
def dataframe_action_to_sql(df: DataFrame):
    """
    This function gives us an example of a dataframe function that we intend to put back into SQL. The only thing
    we need to keep in mind for a SQL return function is that the result has to be a dataframe. Any non-dataframe
    return will result in an error as there's no way for us to know how to upload the object to SQL.
    """
    return df


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
        path="s3://my/s3/path.csv",
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
    )
    agg_orders = aggregate_orders(raw_orders)
    customers = get_customers()
    features = join_orders_and_customers(customers, agg_orders)
    simple_df = perform_dataframe_transformation(df=features)
    # By defining the output_table int the invocation, we are telling astro where to put the result dataframe
    dataframe_action_to_sql(
        simple_df, output_table=Table(table_name="result", conn_id="my_postgres_conn")
    )
```

## Supported databases

The current implementation supports Postgresql and Snowflake. Other databases are on the roadmap. 

To move data from one database to another, you can use the `save_file` and `load_file` functions to store intermediary tables on S3.

## The output_table parameter

### The Table class

To instantiate a table or bring in a table from a database into the `astro` ecosystem, you can pass a `Table` object into the class. This Table object will contain all necessary metadata to handle table creation between tasks. Once you define it in the beginning of your pipeline, `astro` can automatically pass that metadata along.

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

### The TempTable Class

Following the traditional dev ops concept of [pets vs. cattle](http://cloudscaling.com/blog/cloud-computing/the-history-of-pets-vs-cattle/), you can decide whether
the result of a function is a "pet" (e.g. a named table that you would want to reference later), or a "cattle" that can be deleted at any time. 

If you want to ensure that the output of your task is later garbage collected, then declaring it a nameless TempTable will place it into the `astro_tmp` schema, 
which can be later bulk deleted. All `aql.transform` functions will by default output to TempTables unless a `Table` object is used in the `output_table` argument.
```python
from astro import sql as aql
from astro.sql.table import Table, TempTable


@aql.transform
def my_first_sql_transformation(input_table: Table):
    return "SELECT * FROM {input_table}"


@aql.transform
def my_second_sql_transformation(input_table_2: Table):
    return "SELECT * FROM {input_table_2}"


with dag:
    my_table = my_first_sql_transformation(
        input_table=Table(table_name="foo", database="bar", conn_id="postgres_conn"),
        output_table=TempTable(database="bar", conn_id="postgres_conn"),
    )
    my_second_sql_transformation(my_table)
```

## Schemas


By default, our system will create a schema called `tmp_astro` in any database where astro runs, but we also realize that this system works with
two core assumptions. The first assumption is that the data engineer running airflow can create schemas on the fly, and the second is that the user
creating the schema will be the only user adding/removing from said schema.

For production usage we recommend that `astro` users work with their DBAs to create shared schemas where they can put their temporary tables. These schemas
can be shared across multiple users, but should be created with security in mind (e.g. don't place high security data in a shared schema).

Once this schema is created, the Airflow admin can set the schema by setting the `AIRFLOW__ASTRO__SQL_SCHEMA` env variable, or setting the following in their
`airflow.cfg`

```bash
[astro]
sql_schema=<your schema here>
```

## Loading Data

To create an ELT pipeline, users can first load (CSV or parquet) data (from local, S3, or GCS) into a SQL database with the `load_sql` function. 
To interact with S3, set an S3 Airflow connection in the `AIRFLOW__ASTRO__CONN_AWS_DEFAULT` environment variable.

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


## Transform File

Another option for larger SQL queries is to use the `transform_file` function to pass an external SQL file to the DAG.
All of the same templating will work for this SQL query.

```python
with self.dag:
    f = aql.transform_file(
        sql=str(cwd) + "/my_sql_function.sql",
        conn_id="postgres_conn",
        database="pagila",
        parameters={
            "actor": Table("actor"),
            "film_actor_join": Table("film_actor"),
            "unsafe_parameter": "G%%",
        },
        output_table=Table("my_table_from_file"),
    )
```

## Raw SQL
Most ETL use-cases can be addressed by cross-sharing Task outputs, as shown above with `@aql.transform`. For SQL operations that _don't_ return tables but might take tables as arguments, there is `@aql.run_raw_sql`. 

```python
@aql.run_raw_sql
def drop_table(table_to_drop):
    return "DROP TABLE IF EXISTS {table_to_drop}"
```

# Other SQL functions

While simple SQL statements such as `SELECT` statements are very similar between different flavors of SQL, we have found that
certain functions can very widely between different SQL systems. This wide variation can lead to issues if a user decides to switch
from postgres to snowflake. To simplify this process we created some high level APIs that handle certain common SQL use-cases to ensure
universal interoperability of your DAGs across SQL flavors.

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

