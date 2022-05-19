<h1 align="center">
  Astro Python SDK
</h1>
  <h3 align="center">
  Tutorial<br><br>
</h3>

# Introduction

This tutorial demonstrates how to use the Astro Python SDK through a simple ETL example that you can run on your local machine:
* **Extract** a file from S3 into a Snowflake relational table
* **Transform** that table in Snowflake
* **Load** that transformed table into a reporting table in Snowflake

You'll need to use existing Amazon S3 and Snowflake accounts, or create some trial versions.

***
# Let's do some setup

## Install Airflow

For macOS, you can follow these steps:
* Install homebrew: 

    `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
* `brew install pip`
* `brew install python`
* Install Airflow:

    `pip3 install apache-airflow --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.2.5/constraints-3.9.txt"`
* Initialize Airflow: 

    `airflow db init`

## Set up your data stores

### Set up S3

(TODO: add some generic instructions for setting up a trial S3 instance)

Upload this datafile from your local machine to your newly created aws bucket

```
order_id,customer_id,purchase_date,amount
ORDER1,CUST1,1/1/2021,100
ORDER2,CUST2,2/2/2022,200
ORDER3,CUST3,3/3/2023,300
```

### Set up Snowflake

(TODO: Add some generic instructions for setting up a trial Snowflake instance)

## Install Astro on your local machine

* [Install the Astro CLI](https://docs.astronomer.io/astro/install-cli), following the additional steps to [create an Astro project.](https://docs.astronomer.io/astro/create-project)

* In the project directory you just created, create a `.env` file using your favorite file editor:

```shell
AIRFLOW__CORE__ENABLE_XCOM_PICKLING=True
AIRFLOW__ASTRO__SQL_SCHEMA=<snowflake_schema>
```


* Note: prior to running `astrocloud dev start` you'll need to navigate to your Docker Desktop application and double-click on it.

* Check out your Astronomer Airflow UI at `http://localhost:8080/`

## Setup Airflow connections

In your Astronomer Airflow UI select Admin-->Connectors

![connectors](http://localhost:8888/files/Screen%20Shot%202022-05-17%20at%204.05.00%20PM.png?_xsrf=2%7C7f8215bb%7C78c6f1df99528f5087b9d85f0eb5de75%7C1650662527)

### Connect S3 to Airflow

Click on the blue "+" icon to *Add a new record*

Set these fields:

* Connection Id: `aws_default`
* Connection Type: `S3`
* Extra: `{"aws_access_key_id": "<your_access_key", "aws_secret_access_key": "<you_secret_access_key"}`

### Connect Snowflake to Airflow

Click on the blue "+" icon to *Add a new record*

* Connection Id: `snowflake_default`
* Connection Type: `Snowflake`
* Host: `https://<account>.<region>.snowflakecomputing.com/`. This is the URL where you can log into your Snowflake account
* Schema:
* Login:
* Password:
* Account:
* Database:
* Region: (something like us-east-1)
* Role:
* Warehouse:


***
# Create and populate some tables in Snowflake

We'll create some auxiliary tables in Snowflake and populate with a small amount of data for our ETL example.

* Create and populate a customer table to join with an orders table that we create with the Astro Python SDK:

```sql

CREATE OR REPLACE TABLE customers_table (customer_id CHAR(10), customer_name VARCHAR(100), type VARCHAR(10) )

INSERT INTO customers_table (CUSTOMER_ID, CUSTOMER_NAME,TYPE) VALUES ('CUST1','NAME1','TYPE1'),('CUST2','NAME2','TYPE1'),('CUST3','NAME3','TYPE2')
```

* Create and populate a reporting table into which we'll merge our transformed data:

```sql

CREATE OR REPLACE TABLE reporting_table (
    CUSTOMER_ID CHAR(10), CUSTOMER_NAME VARCHAR(100), ORDER_ID CHAR(10), PURCHASE_DATE DATE, AMOUNT FLOAT, TYPE CHAR(10));
    
INSERT INTO reporting_table (CUSTOMER_ID, CUSTOMER_NAME, ORDER_ID, PURCHASE_DATE, AMOUNT, TYPE) VALUES 
('INCORRECT_CUSTOMER_ID','INCORRECT_CUSTOMER_NAME','ORDER2','2/2/2022',200,'TYPE1'),
('CUST3','NAME3','ORDER3','3/3/2023',300,'TYPE2'),
('CUST4','NAME4','ORDER4','4/4/2022',400,'TYPE2')
```
*** 
# Simple ETL workflow

Use your favorite code editor or text editor to copy-paste the following code into a *.py file in <your-project>/dags/ directory.

Here's the code for the simple ETL workflow:

```python

from airflow.decorators import dag
from astro.sql import transform, append, load_file
from astro.sql.table import Table, TempTable
from astro import dataframe as df
from astro import sql as aql
from datetime import datetime
from airflow.models import DAG
import pandas as pd
from pandas import DataFrame
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.snowflake.transfers.s3_to_snowflake import S3ToSnowflakeOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

S3_FILE_PATH = 's3://<aws-bucket-name>'
S3_CONN_ID = 'aws_default'
SNOWFLAKE_CONN_ID = 'snowflake_default'
SNOWFLAKE_ORDERS = 'orders_table'
SNOWFLAKE_FILTERED_ORDERS = 'filtered_table'
SNOWFLAKE_DATABASE = '<database>'
SNOWFLAKE_SCHEMA = '<schema>'
SNOWFLAKE_WAREHOUSE = '<warehouse>'
SNOWFLAKE_JOINED = 'joined_table'
SNOWFLAKE_CUSTOMERS = 'customers_table'
SNOWFLAKE_REPORTING = 'reporting_table'


@aql.transform
def filter_orders (input_table: Table):
   return "SELECT * FROM {{input_table}} WHERE amount > 150"

@aql.transform
def join_orders_customers (filtered_orders_table: Table, customers_table: Table):
    return """SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type 
    FROM {{filtered_orders_table}} fo JOIN {{customers_table}} c 
    ON fo.customer_id = c.customer_id"""

@df
def transform_dataframe(df: DataFrame):
    purchase_dates = df.loc[:,"purchase_date"]
    print('purchase dates:',purchase_dates)
    return purchase_dates
    
dag = DAG(
    dag_id="astro_orders",
    start_date=datetime(2019, 1, 1),
    schedule_interval='@daily',
    catchup=False,
)

with dag:
    
    # Extract a file with a header from S3 into a Table object
    orders_data = aql.load_file(
        # data file needs to have a header row
        path=S3_FILE_PATH + '/orders_data_header.csv',
        file_conn_id=S3_CONN_ID, output_table=Table(database=SNOWFLAKE_DATABASE,conn_id=SNOWFLAKE_CONN_ID)
    )   
    
    # create a Table object for customer data in our Snowflake database
    customers_table =
    Table(table_name=SNOWFLAKE_CUSTOMERS,conn_id=SNOWFLAKE_CONN_ID, database=SNOWFLAKE_DATABASE)
    
    # filter the orders data and then join with the customer table
    joined_data = join_orders_customers(filter_orders(orders_data),customers_table)
    
    # merge the joined data into our reporting table, based on the order_id . 
    # If there's a conflict in the customer_id or customer_name then use the ones from 
    # the joined data
    reporting_table = aql.merge(target_table =   Table(table_name=SNOWFLAKE_REPORTING,conn_id=SNOWFLAKE_CONN_ID,database=SNOWFLAKE_DATABASE,schema=SNOWFLAKE_SCHEMA,warehouse=SNOWFLAKE_WAREHOUSE),     
        merge_table=joined_data,
        merge_columns=["customer_id", "customer_name"],
        target_columns=["customer_id", "customer_name"],
        merge_keys={"order_id": "order_id"},
        conflict_strategy="update")
    
    purchase_dates = transform_dataframe (reporting_table)
```
***
# Run it!
    
In your Astronomer UI's home page, you should see a DAG called astro_orders. Toggle the DAG to unpause it:
    
![toggle](http://localhost:8888/files/Screen%20Shot%202022-05-18%20at%203.43.49%20PM.png?_xsrf=2%7C7f8215bb%7C78c6f1df99528f5087b9d85f0eb5de75%7C1650662527)
    
Then, trigger it to run it:
    
![astro-simple.py](http://localhost:8888/files/Screen%20Shot%202022-05-18%20at%203.39.49%20PM.png?_xsrf=2%7C7f8215bb%7C78c6f1df99528f5087b9d85f0eb5de75%7C1650662527)
    
Click on the astro_orders DAG name to see the tree view of its execution:

![tree-view](http://localhost:8888/files/Screen%20Shot%202022-05-18%20at%204.29.08%20PM.png?_xsrf=2%7C7f8215bb%7C78c6f1df99528f5087b9d85f0eb5de75%7C1650662527)
    
***
# Walk through the code
    
## Extract
    
To extract from S3 into a Table object, we need only specify the location on S3 of the data and a connection for Snowflake that the Python SDK can use for internal purposes:
    
```python
# Extract a file with a header from S3 into a Table object
orders_data = aql.load_file(
    # data file needs to have a header row
    path=S3_FILE_PATH + '/orders_data_header.csv',
    file_conn_id=S3_CONN_ID, output_table=Table(database=SNOWFLAKE_DATABASE,conn_id=SNOWFLAKE_CONN_ID)
)   
```
## Transform
    
We can execute a filter and join in a single line of code to fill in a value for `joined_data`:
    
```python 
@aql.transform
def filter_orders (input_table: Table):
   return "SELECT * FROM {{input_table}} WHERE amount > 150"

@aql.transform
def join_orders_customers (filtered_orders_table: Table, customers_table: Table):
    return """SELECT c.customer_id, customer_name, order_id, purchase_date, amount, type 
    FROM {{filtered_orders_table}} fo JOIN {{customers_table}} c 
    ON fo.customer_id = c.customer_id"""

# create a Table object for customer data in our Snowflake database
customers_table =
Table(table_name=SNOWFLAKE_CUSTOMERS,conn_id=SNOWFLAKE_CONN_ID, database=SNOWFLAKE_DATABASE)

# filter the orders data and then join with the customer table
joined_data = join_orders_customers(filter_orders(orders_data),customers_table)
```
    
## Merge
    
As our penultimate step, we call a database-agnostic merge function:
    
```python
    # merge the joined data into our reporting table, based on the order_id . 
    # If there's a conflict in the customer_id or customer_name then use the ones from 
    # the joined data
    reporting_table = aql.merge(target_table =   Table(table_name=SNOWFLAKE_REPORTING,conn_id=SNOWFLAKE_CONN_ID,database=SNOWFLAKE_DATABASE,schema=SNOWFLAKE_SCHEMA,warehouse=SNOWFLAKE_WAREHOUSE),     
        merge_table=joined_data,
        merge_columns=["customer_id", "customer_name"],
        target_columns=["customer_id", "customer_name"],
        merge_keys={"order_id": "order_id"},
        conflict_strategy="update")
```   

## Dataframe transformation

As an illustration of the `@df` decorator, we show a simple dataframe operation:
    
```python
@df
def transform_dataframe(df: DataFrame):
    purchase_dates = df.loc[:,"purchase_date"]
    print('purchase dates:',purchase_dates)
    return purchase_dates
```
    
    
After all that, you'll find the meager output of this example in the logs of the final task:
    
![log](http://localhost:8888/files/Screen%20Shot%202022-05-19%20at%2010.16.06%20AM.png?_xsrf=2%7C7f8215bb%7C78c6f1df99528f5087b9d85f0eb5de75%7C1650662527)    
    
To view this log output, in the tree view of the DAG, click on the green box next to `transform_dataframe` and then on "Log" button.

