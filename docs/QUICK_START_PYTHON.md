
# Getting Started With Astro


## Downloading astro

Downloading astro is no different from installing any other python library. It requires Airflow 2.1+, and can be installed via pip either directly in the command line or in your requirements.txt file


### Option 1: pip install


```
pip install astro-projects
```



### Option 2: requirements.txt


```
astro-projects
```



## Setting Environment Variables

As fair as Airflow variables, the only airflow specific variable needed is to set xcom pickling to True. This will allow us to pass rich information between tasks that will improve your DAG writing experience.

To turn on xcom_pickling, you can set the following environment variable:


```
AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
```



### Alternative: set value in airflow.cfg


```
[core]
enable_xcom_pickling = true
```



## Setting a Temporary Schema

When processing SQL-based DAGs, Astro creates a lot of temporary tables. These tables allow SQL outputs to be inherited by other tasks and inspected for data quality. That said, we realize that creating a lot of temporary tables can lead to a situation where cleanup would be somewhat difficult. To address this concern, we store all temporary tables in a “temporary schema” that a DBA can delete in a single command. By default the name of the temporary schema is “tmp_astro”, but you can change this value to match the needs of your airflow application (e.g. a dev airflow could have a “tmp_astro_dev” schema).

To set your custom schema add the following environment variable:


```
AIRFLOW__ASTRO__SQL_SCHEMA="my_tmp_schema",
```


Note: \


Depending on your level of permissions you might need to speak with your dba about setting up a temporary schema for you ahead of time.


### Alternative: set value in airflow.cfg


```
[astro]
sql_schema="my_tmp_schema"
```



# Creating a test postgres instance

For the sake of this demo, let’s create a local postgres instance that is pre-populated with the [pagila](https://dataedo.com/samples/html/Pagila/doc/Pagila_10/home.html) dataset. This is a standard open dataset provided by postgres that has a number of movies, actors, and directors.

To set up pagila on your local machine, run the following docker command:


```shell script
docker run --rm -it -p 5433:5432 dimberman/pagila-test &
```



# Getting Up and Running


## Your First Astro DAG

Now that we have gotten all of the system-level work out of the way, let’s get started with our first DAG!

To begin your astro journey, please copy the following DAG into a python file in your DAG. You can name this file whatever you like, but we recommend you call it astro_dag.py


```python
import os
from datetime import datetime, timedelta

from airflow.models import DAG

from astro import sql as aql
from astro.sql.table import Table
from astro.dataframe import dataframe as adf
import pandas as pd

DAG_ID = "my_astro_dag"

default_args = {
    "retries": 1,
    "retry_delay": 0,
}

dag = DAG(
    dag_id=DAG_ID,
    start_date=datetime(2019, 1, 1),
    max_active_runs=3,
    schedule_interval=None,
    default_args=default_args,
)


@aql.transform
def test_astro():
    return "SELECT * FROM actor"


@aql.transform
def test_inheritance(my_astro_table: Table):
    return "SELECT * FROM {{my_astro_table}} LIMIT 10"


@adf
def my_dataframe_func(df: pd.DataFrame):
    print(df.to_string)


dir_path = os.path.dirname(os.path.realpath(__file__))
with dag:
    actor_table = test_astro(database="pagile", conn_id="my_postgres_conn")
    inherited_table = test_inheritance(my_astro_table=actor_table)
    my_dataframe_func(df=inherited_table)
```



Please note that you can set the DAG_ID variable to change the DAG ID in Airflow, but this is optional. Once you have pasted this code into your file, you’ll notice that Airflow can now find your DAG, but there are no tasks, so let’s make some!

## The Table class

To create or import a SQL table in the `astro` ecosystem, you can create a `Table`. The `Table` object contains all of the metadata that's necessary for handling SQL table creation between Airflow tasks. `astro` can automatically pass metadata from a `Table` to downstream tasks, meaning that you only need to define your database context once at the start of your DAG.

At a minimum, a `Table` can have the following arguments:

- `conn_id` (_required_): The Airflow connection ID for the database
- `name`: The name of the table
- `database`: The database where this table is stored or should be stored

Connections for certain databases (such as Snowflake), might contain more values such as `warehouse` and `role`.

You will also note that there is a `Table` and a `TempTable` class. Consider this as the difference between a `pet` and a `cattle`.
TempTables will be defaulted to your temporary schema where they can be easily deleted by your DBA, while Tables are assumed to refer
to long-living data.

So for example, if you want to load a file into a postgres database, you might do something like this:

```python
from astro import sql as aql
from astro.sql.table import Table, TempTable

my_homes_table = aql.load_file(
    path="s3://tmp9/homes.csv",
    output_table=TempTable(
        database="pagila",
        conn_id="postgres_conn",
    ),
)
```

## Loading Data

To create an ELT pipeline, users can first load CSV or parquet data from either local, S3, or GCS into a SQL database with the `load_sql` function.
To interact with S3, you must set an S3 Airflow connection in the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY`  environment variables.

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


### Raw SQL

Most ETL use cases can be addressed by cross-sharing task outputs, as shown above with `@aql.transform`. If you need to perform a SQL operation that doesn't return a table but might take a table as an argument, you can use `@aql.run_raw_sql`.

```python
@aql.run_raw_sql
def drop_table(table_to_drop):
    return "DROP TABLE IF EXISTS {{table_to_drop}}"
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
    return "SELECT * FROM {{input_table}}"


with self.dag:
    my_df = get_dataframe(
        output_table=Table(
            table_name="my_df_table", conn_id="postgres_conn", database="pagila"
        )
    )
    pg_df = sample_pg(my_df)
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
        FROM {[customer_table}} WHERE NOT is_deleted"""


@aql.transform
def join_orders_and_customers(orders_table: Table, customer_table: Table):
    """Now join those together to create a very simple 'feature' dataset."""
    return """SELECT c.customer_id, c.source, c.region, c.member_since,
        CASE WHEN purchase_count IS NULL THEN 0 ELSE 1 END AS recent_purchase
        FROM {{orders_table}} c LEFT OUTER JOIN {{customer_table}} p ON c.customer_id = p.customer_id"""


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
    conflict_strategy="update",
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
    conflict_strategy="ignore",
)
```

## Truncate table

```python
a = aql.truncate(
    table=TRUNCATE_TABLE,
)
```
