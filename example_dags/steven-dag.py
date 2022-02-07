"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
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
    score_model(model=model, df=new, output_table=Table("", ""))
