<p align="center">
  <a href="https://www.airflow.apache.org">
    <img alt="Airflow" src="https://cwiki.apache.org/confluence/download/attachments/145723561/airflow_transparent.png?api=v2" width="60" />
  </a>
</p>
<h1 align="center">
  Astro SDK Python :rocket:
</h1>
  <h3 align="center">
  Your new Airflow DAG writing experience. Maintained with ❤️ by Astronomer.
</h3>
<br/>

# :mega: Disclaimer :mega:
```
This project is in a preview release. Interfaces may change with feedback from early users.
Though the code undergoes automated testing, it is not yet ready for production.
```
# Overview

The Astro SDK Python library simplifies writing ETL and ELT workflows in Airflow, allowing Python engineers to focus on writing Python, and all engineers focus on data engineering instead of configuration. By design, `astro-sdk-python` modules automatically pass database contexts to your tasks, meaning that you can focus on writing code and leave metadata definitions for load time.

- **Without `astro-sdk-python`**: Database connections and context are defined in operators. This metadata does not pass automatically from task to task, meaning that you have to redefine it for each new task/ operator. SQL queries must be static.
- **With `astro-sdk-python`**: Database connections and context are defined at load time. Metadata is automatically passed into functions, meaning that you can write generic queries in your task and run these queries without knowing the context ahead of time. SQL queries can be templated with JINJA.

# Philosophy

With the `astro-sdk-python` library, we want to redefine the DAG writing experience from the bottom up. Our goal is to empower
data engineers to write DAGs based around _data_ instead of _task dependencies_.
With this in mind, we built a library focused on data movement and simplifying data transformations between different environments. Our first two integrations are SQL and pandas, but we are planning many more in the coming months.

With our SQL module, you should have the ability to treat SQL tables as if they're python objects. You can manipulate them,
join them, templatize them, and ultimately turn them into dataframes if you want to run python functions against them. We hope that this
library creates a cleaner Airflow ELT experience, as well as an easier on-boarding for those who want to focus on data transformations
instead of DAGs.

Please feel free to raise issues and propose improvements. Community contributions are highly welcome!

Thank you,

:sparkles: The Astro SDK Team :sparkles:

# Setup

To start using `astro-sdk-python`:

1. Install `astro` by running the following command:

    ```shell script
    pip install astro-sdk-python
    ```

    Alternatively, you can add `astro-sdk-python` to your `requirements.txt` file.


2. Installing `astro-sdk-python` with extras(i.e., gcp, snowflake, postgres)

    ```shell script
    pip install astro-sdk-python[google,snowflake,postgres]
    ```

3. Set the following environment variable so that `astro` can pass table objects between tasks:

    ```shell script
    AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
    ```

# Usage

## Setting Input and Output Tables

Before we can complete any transformations, we need to define a way to get our tables in and out of Airflow. We can do this by defining either `Table` or `TempTable` objects in the `input_table` and `output_table` parameters of our table instantiations.

### The Table class

To instantiate a table or bring in a table from a database into the `astro-sdk-python` ecosystem, you can pass a `Table` object into the class. This `Table` object will contain all of the metadata that's necessary for handling table creation between tasks. After you define a Table's metadata in the beginning of your pipeline, `astro-sdk-python` can automatically pass that metadata along to downstream tasks.

If the user does not define the metadata properties within table, the `astro-sdk-python` attempts to retrieve them from the connection associated with `conn_id`. If the database supports schemas and the connection doesn't specify a schema, the SDK will retrieve the schema from the `AIRFLOW__ASTRO__SQL_SCHEMA` environment variable.

In the following example, we define our table in the DAG instantiation. In each subsequent task, we pass in only an input `Table` argument because `astro-sdk-python` automatically passes in the additional context from our original `input_table` parameter.

```python
from datetime import datetime

from airflow.models import DAG

from astro import sql as aql
from astro.sql.table import Metadata, Table


@aql.transform
def my_first_sql_transformation(input_table: Table):
    return "SELECT * FROM {{input_table}}"


@aql.transform
def my_second_sql_transformation(input_table_2: Table):
    return "SELECT * FROM {{input_table_2}}"


dag = DAG(
    dag_id="astro_sdk_example_1",
    start_date=datetime(2019, 1, 1),
    schedule_interval=None,
)


with dag:
    my_table = my_first_sql_transformation(
        input_table=Table(
            name="foo", conn_id="postgres_conn", metadata=Metadata(schema="postgres")
        )
    )
    my_second_sql_transformation(my_table)
```

### The TempTable Class

Following the traditional dev ops concept of [pets vs. cattle](http://cloudscaling.com/blog/cloud-computing/the-history-of-pets-vs-cattle/), you can decide whether the result of a function is a "pet" (e.g. a named table that you would want to reference later), or a "cattle" that can be deleted at any time for garbage collection.

If you want to ensure that the output of your task is a cattle, you can declare `Table(temp=True,...)` or `Table()`. If a table name is not given, the SDK generates a table name, prefixed with `_tmp_` and assumes the table is temporary.
By default, all `aql.transform` functions will output to temporary tables unless the user sets the argument `output_table` with a named `Table` instance.

At the moment the `astro-sdk-python` does not expose a tool to clean up temporary tables. This can be achieved by the user running a function similar to:

```python
def cleanup(hook):
    eng = hook.get_sqlalchemy_engine()
    for table_name in eng.table_names():
        if table_name.startswith("_tmp_"):
            hook.run(f"DROP TABLE IF EXISTS {table_name}")
```

In the following example DAG, we set an `output_table` to a nameless `Table()` meaning that the `astro-sdk-python` will give it a name and consider it temporary.

```python
from astro import sql as aql
from astro.sql.table import Table, TempTable


@aql.transform
def my_first_sql_transformation(input_table: Table):
    return "SELECT * FROM {{input_table}}"


@aql.transform
def my_second_sql_transformation(input_table_2: Table):
    return "SELECT * FROM {{input_table_2}}"


with dag:
    my_table = my_first_sql_transformation(
        input_table=Table(name="foo", conn_id="postgres_conn"),
        output_table=Table(),
    )
    my_second_sql_transformation(my_table)
```

## Loading Data

To create an ELT pipeline, users can first load CSV or parquet data from either local, S3, or GCS into a SQL database with the `load_file` function.
The credentials to Google Cloud Storage and Amazon S3 should be set using Airflow Connections.

In the following example, we load data from S3 by specifying the path and connection ID for our S3 database in `aql.load_file`:

```python
from astro import sql as aql
from astro.sql.table import Table

raw_orders = aql.load_file(
    input_file=File(path="s3://my/s3/path.csv", conn_id="my_s3_conn"),
    output_table=Table(name="my_table", conn_id="postgres_conn"),
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


### Raw SQL

Most ETL use cases can be addressed by cross-sharing task outputs, as shown above with `@aql.transform`. If you need to perform a SQL operation that doesn't return a table but might take a table as an argument, you can use `@aql.run_raw_sql`.

```python
@aql.run_raw_sql
def drop_table(table_to_drop):
    return "DROP TABLE IF EXISTS {{table_to_drop}}"
```

## Putting it All Together

The following is a full example DAG of a SQL + Python workflow using the `astro-sdk-python`. We pull data from S3, run SQL transformations to merge our pulled data with existing data, and move the result of that merge into a dataframe so that we can complete complex work on it using Python / ML.

```python
from datetime import datetime, timedelta

from airflow.models import DAG
from pandas import DataFrame

from astro.file import File
from astro import sql as aql
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


@aql.transform(conn_id="postgres_conn")
def get_customers(customer_table: Table = Table("customer")):
    """Basic clean-up of an existing table."""
    return """SELECT customer_id, source, region, member_since
        FROM {[customer_table}} WHERE NOT is_deleted"""


@aql.transform
def join_orders_and_customers(orders_table: Table, customer_table: Table):
    """Now join those together to create a very simple 'feature' dataset."""
    return """SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {{orders_table}} c LEFT OUTER JOIN {{customer_table}} p ON c.customer_id = p.customer_id"""


@aql.dataframe
def perform_dataframe_transformation(df: DataFrame):
    """Train model with Python. You can import any python library you like and treat this as you would a normal
    dataframe
    """
    recent_purchases_dataframe = df.loc[:, "recent_purchase"]
    return recent_purchases_dataframe


@aql.dataframe
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
        File("s3://my/s3/path.csv", "my_s3_conn"),
        Table(name="foo", conn_id="my_postgres_conn"),
    )
    agg_orders = aggregate_orders(raw_orders)
    customers = get_customers()
    features = join_orders_and_customers(customers, agg_orders)
    simple_df = perform_dataframe_transformation(df=features)
    # By defining the output_table in the invocation, we are telling astro where to put the result dataframe
    dataframe_action_to_sql(
        simple_df, output_table=Table(table="result", conn_id="my_postgres_conn")
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
    append_table=Table("some_table", "snowflake_default"),
    main_table=Table("main_table", "snowflake_default"),
    columns=["Bedrooms", "Bathrooms"],
    casted_columns={"Age": "INTEGER"},
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
    target_table=Table("target_table", "postgres_default"),
    merge_table=Table("merge_table", "postgres_default"),
    merge_keys=["list", "sell"],
    target_columns=["list", "sell", "taxes"],
    merge_columns=["list", "sell", "age"],
    conflict_strategy="update",
)
```
Snowflake:
```python
a = aql.merge(
    target_table=Table("target_table", "snowflake_default"),
    merge_table=Table("merge_table", "snowflake_default"),
    merge_keys={"list": "list", "sell": "sell"},
    target_columns=["list", "sell"],
    merge_columns=["list", "sell"],
    conflict_strategy="ignore",
)
```

## Truncate table

```python
a = aql.truncate(
    table=Table("name", "sqlite_default"),
)
```

# Dataframe functionality

Finally, your pipeline might call for procedures that would be too complex or impossible in SQL. This could be building a model from a feature set, or using a windowing function which Pandas is more adept for. The `df` functions can easily move your data into a Pandas dataframe and back to your database as needed.

At runtime, the operator loads any `Table` object into a Pandas DataFrame. If the Task returns a DataFame, downstream Taskflow API Tasks can interact with it to continue using Python.

If after running the function, you wish to return the value into your database, simply include a `Table` in the reserved `output_table` parameters (please note that since this parameter is reserved, you can not use it in your function definition).

## dataframe
```python
from astro import sql as aql
from astro.sql.table import Metadata, Table
import pandas as pd


@aql.dataframe
def get_dataframe():
    return pd.DataFrame({"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]})


@aql.transform
def sample_pg(input_table: Table):
    return "SELECT * FROM {{input_table}}"


with dag:
    my_df = get_dataframe(
        output_table=Table(
            name="my_df_table",
            conn_id="postgres_conn",
            metadata=Metadata(database="pagila"),
        )
    )
    pg_df = sample_pg(my_df)
```
