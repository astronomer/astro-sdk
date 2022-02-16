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

# [START tutorial]
# [START import_module]

from airflow.decorators import dag, task
from airflow.utils.dates import days_ago

import astro.sql as aql
from astro.sql.table import Table

# [END import_module]

# [START default_args]
# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    "owner": "airflow",
    "email": ["vikram@astronomer.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
}
# [END default_args]


# [START instantiate_dag]
@dag(default_args=default_args, schedule_interval=None, start_date=days_ago(2))
def tutorial_elt_sql():
    """
    ### Tutorial ELT Dag with the SQL Decorator
    This is a simple ELT data pipeline example which demonstrates the use of
    the SQL Decorator and the TaskFlow API using three simple tasks for Extract,
    Load, and Transform.
    """
    # [END instantiate_dag]

    # [START extract]
    @task()
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, a simulated CSV data file is created with data
        from this task.
        """

        order_data_file = "/tmp/order_data.csv"

        tmp_data_file = open(order_data_file, "w")
        tmp_data_file.write("order_id,order_value\n")
        tmp_data_file.write('"1001", 301.27\n')
        tmp_data_file.write('"1002", 433.21\n')
        tmp_data_file.write('"1003", 502.22\n')
        tmp_data_file.close()

        return order_data_file

    # [END extract]

    # [START load]
    @aql.transform(postgres_conn_id="postgres_conn", database="testdata")
    def load(csv_path=None, input_table=None, output_table=None):
        """
        #### Load task
        A simple Load task to load the CSV file data into a SQL table
        using the SQL decorator
        """
        return """SELECT * FROM {{input_table}}"""

    # [END load]

    # [START transform]
    @aql.transform(postgres_conn_id="postgres_conn", database="testdata")
    def transform(input_table=None, output_table=None):
        """
        #### Transform task
        A simple Transform task to get the summarize all the order values from
        the order data table into a total order value. This creates a new summary table
        in the database.
        This demonstrates the transformation use of the SQL Decorator
        """
        return """SELECT sum(order_value) FROM {{input_table}} LIMIT 1"""

    # [END transform]

    # [START main_flow]

    # in the example just generate the expected data file, so there are no
    # external dependencies needed to get the data file

    order_data_file = extract()
    order_data = load(
        csv_path=order_data_file,
        input_table=Table(
            table_name="my_input_table", database="pagila", conn_id="postgres_conn"
        ),
    )
    order_summary = transform(input_table=order_data)
    # print_table(input_table=order_summary)

    # [END main_flow]


# [START dag_invocation]
tutorial_elt_sql_dag = tutorial_elt_sql()
# [END dag_invocation]

# [END tutorial]
