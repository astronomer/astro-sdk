<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->
**Table of Contents**  *generated with [DocToc](https://github.com/thlorenz/doctoc)*

- [
  Astro :rocket:
](#astro-rocket)
- [Overview](#overview)
- [Philosophy](#philosophy)
- [:mega: Disclaimer :mega:](#mega-disclaimer-mega)
- [Setup](#setup)
- [Using Astro as a SQL Engineer](#using-astro-as-a-sql-engineer)
  - [Schemas](#schemas)
  - [Setting up SQL files](#setting-up-sql-files)
- [Using Astro as a Python Engineer](#using-astro-as-a-python-engineer)
  - [Setting Input and Output Tables](#setting-input-and-output-tables)
  - [Loading Data](#loading-data)
  - [Transform](#transform)
  - [Putting it All Together](#putting-it-all-together)
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

# Overview

The astro library is a suite of tools for writing ETL and ELT workflows in Airflow. It lets SQL engineers focus on writing SQL, Python engineers focus on writing Python, and all engineers focus on data engineering instead of configuration. By design, `astro` modules automatically pass database contexts to your tasks, meaning that you can focus on writing code and leave metadata definitions for load time.

- **Without `astro`**: Database connections and context are defined in operators. This metadata does not pass automatically from task to task, meaning that you have to redefine it for each new task/ operator. SQL queries must be static. 
- **With `astro`**: Database connections and context are defined at load time. Metadata is automatically passed into functions, meaning that you can write generic queries in your task and run these queries without knowing the context ahead of time. SQL queries can be templated with JINJA.

# Philosophy

With the `astro` library, we want to redefine the DAG writing experience from the bottom up. Our goal is to empower
data engineers and data scientists to write DAGs based around _data_ instead of _task dependencies_.
With this in mind, we built a library focused on data movement and simplifying data transformations between different environments. Our first two integrations are SQL and pandas, but we are planning many more in the coming months.

With our SQL and dataframe modules, you should have the ability to treat SQL tables as if they're python objects. You can manipulate them,
join them, templatize them, and ultimately turn them into dataframes if you want to run python functions against them. We hope that this
library creates a cleaner Airflow ELT experience, as well as an easier onboarding for those who want to focus on data transformations
instead of DAGs. 

Please feel free to raise issues and propose improvements. Community contributions are highly welcome!

Thank you,

:sparkles: The Astro Team :sparkles:

# :mega: Disclaimer :mega:
This project is still very early and the API will probably change as it progresses. We are actively seeking alpha users and brave souls to test it and offer feedback, but please know that this is not yet ready for production.

# Setup

To start using `astro`:

1. Install `astro` by running the following command:

    ```shell script
    pip install astro-projects
    ```

    Alternatively, you can add `astro-projects` to your `requirements.txt` file.

2. Set the following environment variable so that `astro` can pass table objects between tasks:

    ```shell script
    AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
    ```

#  Using Astro as a SQL Engineer

## Schemas


By default, running `astro` for a given database creates a schema called `tmp_astro` in the database. This default behavior assumes that you have permissions to create schemas on the fly, and that there is only one user adding/removing from this schema.

For production usage, we recommend that you work with your database administrator to create a shared schema for temporary tables. This schema
can be shared across multiple users, but should be created with security in mind (e.g. don't place high security data in a shared schema).

Once you create this schema, an Airflow admin can replace `tmp_astro` by setting `AIRFLOW__ASTRO__SQL_SCHEMA="<new-temp-schema>"`, or by setting the following in `airflow.cfg`:

```bash
[astro]
sql_schema=<new-temp-schema>
```

## Setting up SQL files

When writing out a SQL DAG using `astro`, you can think of each SQL file as its own Airflow task. For example, if you wanted
to aggregate orders, aggregate customers, and then join customers and orders, you could have the following directory of files:

```
|
ingest_models/
|
 -- customers_table.sql
 -- orders_table.sql
 -- join_customers_and_orders.sql
```

In each of these SQL files, standard `SELECT` statements automatically creates a table that can be referenced in downstream SQL files via a data dependency. `astro` handles creating all of the temporary tables required for this process.
```sql
# join_customers_and_orders.sql
SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM orders c LEFT OUTER JOIN customers p ON c.customer_id = p.customer_id
```

### Defining metadata

Once your SQL is working as expected, you might want to define the query's database and schema during its runtime.
To configure this for Airflow while keeping your SQL easy to run in your favorite SQL notebook, 
you can create a [frontmatter](https://middlemanapp.com/basics/frontmatter/): 

```sql
# join_customers_and_orders.sql
---
database: foo
schema: bar
---
SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM orders c LEFT OUTER JOIN customers p ON c.customer_id = p.customer_id
```

One benefit of putting all metadata into a frontmatter block is that all of your SQL is still valid outside of the context of Airflow. If you want to develop your SQL locally, comment out the frontmatter block.

```sql
# join_customers_and_orders.sql
-- ---
-- database: foo
-- schema: bar
-- ---
SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM orders c LEFT OUTER JOIN customers p ON c.customer_id = p.customer_id
``` 

### Defining dependencies

When running SQL queries in Airflow DAGs, you need to define dependencies that break up your SQL into
multiple, reproducible steps. We offer two ways to define dependencies within an `astro` SQL file:

You can define your dependency via the `template_vars` frontmatter argument. In the following example, we set `template_vars` so that our `join_customers_and_orders` query cannot run until both our `customers_table` and `agg_orders` queries finish. This data dependency is equivalent to a task dependency in our DAG. The only difference is that we're defining it directly in our SQL instead of using Airflow's dependency operators. 

```sql
# join_customers_and_orders.sql
---
database: foo
schema: bar
template_vars:
    customers: customers_table
    orders: agg_orders
---
SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM orders c LEFT OUTER JOIN customers p ON c.customer_id = p.customer_id
```

### Defining outputs

With certain SQL models, you will want to specify an output based on a table name, schema, and/or database. 

Any table created without an `output_table` will be placed in the temporary schema with a generated table name.

```sql
---
database: foo
schema: bar
template_vars:
    customers: customers_table
    orders: agg_orders
output_table:
    table_name: my_pg_table
    database: foo
    schema: my_prod_schema
---
SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM orders c LEFT OUTER JOIN customers p ON c.customer_id = p.customer_id
```
### Supported arguments

Here is a list of supported frontmatter arguments:

| Argument      | Description |
| ----------- | ----------- |
| conn_id | The connection that this query should run against |
| Database      | The database to query    |
| Schema   | The schema to query. Default value is either `tmp_astro` or your temp schema defined in `AIRFLOW__ASTRO__SQL_SCHEMA`    |
| template_vars | A key-value dictionary of what values to override when this SQL file is used in a DAG.  |
| output_table | Specs of location and table name for tables that want to be treated as "pets" insteaad of "cattle" | 

### Incorporating SQL directory into DAG

Now that you have developed your SQL, we can attach your directory to any DAG using the `aql.render` function.

The following example DAG pulls a directory of CSV files from s3 into a postgres table, then passes
that table into a directory of SQL models that will process the table in an ELT fashion.

```python
import os
from datetime import datetime, timedelta

from airflow.models import DAG

from astro import sql as aql
from astro.sql.table import Table

default_args = {
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id="sql_file_dag",
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
)


dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    raw_orders = aql.load_file(
        path="s3://my/path/{{ execution_date }}/",
        task_id="pull_from_s3",
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
    )
    ingest_models = aql.render(dir_path + "/ingest_models", orders_table=raw_orders)
```

Once the `render` function completes, it returns a dictionary of all of the models based on the sql file name.
If you have a sql file named `join_orders_and_customers.sql`, then the result would be stored in `model["join_orders_and_customers]`.

### Passing on tables to subsequent tasks

The following DAG is an example of passing a single model to a subsequent rendering (perhaps you want to separate your ingest and transforms):

```python
dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    raw_orders = aql.load_file(
        path="s3://my/path/{{ execution_date }}/",
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
    )
    ingest_models = aql.render(dir_path + "/ingest_models", orders_table=raw_orders)
    aql.render(
        dir_path + "/transform_models",
        orders_and_customers=ingest_models["join_orders_and_customers"],
    )
```

You can also pass the entire dictionary of models to the subsequent task by dereferencing the dictionary using `**`.

In this example, we pass _all_ tables to the next round of SQL files.

```python
dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    raw_orders = aql.load_file(
        path="s3://my/path/{{ execution_date }}/",
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
    )
    ingest_models = aql.render(dir_path + "/ingest_models", orders_table=raw_orders)
    aql.render(
        dir_path + "/transform_models",
        orders_and_customers=ingest_models["join_orders_and_customers"],
    )
```

You can even pass the resulting tables into a python function that uses the `astro.dataframe` to
automatically convert your table into a dataframe. We'll discuss how to do this more in "Using Astro as a Python Engineer".

```python
from astro.dataframe import dataframe as df


@df
def aggregate_data(agg_df: pd.DataFrame):
    customers_and_orders_dataframe = agg_df.pivot_table(
        index="DATE", values="NAME", columns=["TYPE"], aggfunc="count"
    ).reset_index()
    return customers_and_orders_dataframe


dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    raw_orders = aql.load_file(
        path="s3://my/path/{{ execution_date }}/",
        file_conn_id="my_s3_conn",
        output_table=Table(table_name="foo", conn_id="my_postgres_conn"),
    )
    ingest_models = aql.render(dir_path + "/ingest_models", orders_table=raw_orders)
    aggregate_data(agg_df=ingest_models["agg_orders"])
```


# Using Astro as a Python Engineer

For those who don't want to store their transformations in external SQL files or who want to create transformation
functions that are extendable and importable, we offer a rich python API that simplifies the SQL experience for the python engineer!

## Setting Input and Output Tables

Before we can complete any transformations, we need to define a way to get our tables in and out of Airflow. We can do this by defining either `Table` or `TempTable` objects in the `input_table` and `output_table` parameters of our table instantiations.

### The Table class

To instantiate a table or bring in a table from a database into the `astro` ecosystem, you can pass a `Table` object into the class. This Table object will contain all of the metadata that's necessary for handling table creation between tasks. After you define a Table's metadata in the beginning of your pipeline, `astro` can automatically pass that metadata along to downstream tasks.

In the following example, we define our table in the DAG instantiation. In each subsequent task, we only pass in an input table argument because `astro` automatically passes in the additional context from our original `input_table` parameter.

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

Following the traditional dev ops concept of [pets vs. cattle](http://cloudscaling.com/blog/cloud-computing/the-history-of-pets-vs-cattle/), you can decide whether the result of a function is a "pet" (e.g. a named table that you would want to reference later), or a "cattle" that can be deleted at any time for garbage collection. 

If you want to ensure that the output of your task is a cattle, you can declare it as a nameless `TempTable`. This places the output into your temp schema, 
which can be later bulk deleted. By default, all `aql.transform` functions will output to TempTables unless a `Table` object is used in the `output_table` 
argument.

In the following example DAG, we set an `output_table` to a nameless `TempTable` meaning that any output from this DAG will be deleted once the DAG completes. If we wanted to keep our output, we would simply update the parameter to instantiate a `Table` instead.


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

## Loading Data

To create an ELT pipeline, users can first load CSV or parquet data from either local, S3, or GCS into a SQL database with the `load_sql` function. 
To interact with S3, you must set an S3 Airflow connection in the `AIRFLOW__ASTRO__CONN_AWS_DEFAULT` environment variable.

In the following example, we load data from S3 by specifying the path and connection ID for our S3 database in `aql.load_file`: 

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

The `transform` function of the SQL decorator is the "T" of the ELT system. Each step of the transform pipeline creates a new table from the
`SELECT` statement and enables tasks to pass those tables as if they were native Python objects. The following example DAG shows how we can quickly pass tables between tasks when completing a data transformation.


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

Note that the functions in this example use a custom templating system. Wrapping a value in single brackets 
(like `{customer_table}`) indicates the value needs to be rendered as a SQL table. The SQL decorator
also treats values in double brackets as Airflow jinja templates. 

Please note that this is NOT an f string. F-strings in SQL formatting risk security breaches via SQL injections. 

For security, users MUST explicitly identify tables in the function parameters by typing a value as a `Table`. Only then will the SQL decorator treat the value as a table. 


### Transform File

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

### Raw SQL

Most ETL use cases can be addressed by cross-sharing task outputs, as shown above with `@aql.transform`. If you need to perform a SQL operation that doesn't return a table but might take a table as an argument, you can use `@aql.run_raw_sql`. 

```python
@aql.run_raw_sql
def drop_table(table_to_drop):
    return "DROP TABLE IF EXISTS {table_to_drop}"
```

## Putting it All Together 

The following is a full example DAG of a SQL + Python workflow using `astro`. We pull data from S3, run SQL transformations to merge our pulled data with existing data, and move the result of that merge into a dataframe so that we can complete complex work on it using Python / ML.

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
    # By defining the output_table in the invocation, we are telling astro where to put the result dataframe
    dataframe_action_to_sql(
        simple_df, output_table=Table(table_name="result", conn_id="my_postgres_conn")
    )
```

# Other SQL functions

While simple SQL statements such as `SELECT` statements are very similar between different flavors of SQL, certain functions can vary widely between different SQL systems. This wide variation can lead to issues if a user decides to switch from postgres to snowflake. To simplify this process, we created some high level APIs that handle certain common SQL use-cases to ensure universal interoperability of your DAGs across SQL flavors.

## Appending data

After transforming a table, you might want to append the results of your transformation to a reporting table. For example, you might
want to aggregate daily data on a "main" table that analysts use for timeseries analysis. 

The `aql.append` function merges tables assuming that there are no conflicts. You can choose to merge the data 'as-is' or cast it to a new value if needed. Note that this query will fail if there is a merge conflict.

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

To merge data into an existing table in situations where there might be conflicts, the `aql.merge` function
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

Finally, your pipeline might call for procedures that would be too complex or impossible in SQL. This could be building a model from a feature set, or using a windowing function which Pandas is more adept for. The `df` functions can easily move your data into a Pandas dataframe and back to your database as needed.

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

