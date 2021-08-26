"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_sample_operator.TestSampleOperator

"""

import json
import logging
import unittest.mock
from unittest import mock
from pandas import DataFrame

import requests_mock
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.models import Connection
import os

# Import Operator
from astronomer_sql_decorator.operators.snowflake_decorator import snowflake_decorator

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_CONN_SAMPLE='http://https%3A%2F%2Fwww.httpbin.org%2F')
class TestSampleOperator(unittest.TestCase):
    """
    Test Sample Operator.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(Connection).delete()
            postgres_connection = Connection(
                conn_id="snowflake_conn",
                conn_type="snowflake",
                host="https://gp21411.us-east-1.snowflakecomputing.com",
                login=os.environ["SNOW_ACCOUNT_NAME"],
                port=443,
                password=os.environ["SNOW_PASSWORD"],
                extra={
                    "account": "gp21411",
                    "region": "us-east-1",
                    "role": "DANIEL",
                },

            )
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(Connection).delete()
            session.add(postgres_connection)

    def setUp(self):
        super().setUp()
        self.dag = DAG('test_dag', default_args={'owner': 'airflow', 'start_date': DEFAULT_DATE})
        self.addCleanup(self.dag.clear)
        self.clear_run()
        self.addCleanup(self.clear_run)

    def clear_run(self):
        self.run = False

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_snow_dataframe_func(self):
        @snowflake_decorator(snowflake_conn_id="snowflake_conn",
                             warehouse="REPORTING_DEV",
                             database="DWH_LEGACY",
                             to_dataframe=True)
        def get_df(input_df: DataFrame):
            return input_df

        with self.dag:
            # Injecting a subquery as we do not want to pull this entire table.
            # DO NOT DO THIS ON YOUR DAGS!
            x = get_df(input_table="(WITH my_cte AS (SELECT * "
                                   "FROM REPORTING.CLOUD_USAGE "
                                   "LIMIT 10) SELECT * FROM my_cte)")

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        x.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        ti = dr.get_task_instances()[0]
        assert len(ti.xcom_pull()) == 10

    def test_postgres(self):
        @snowflake_decorator(snowflake_conn_id="snowflake_conn", database="pagila")
        def sample_pg(input_table):
            return "SELECT * FROM %(input_table)s WHERE last_name LIKE 'G%%'"

        with self.dag:
            f = sample_pg("actor")

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)


if __name__ == '__main__':
    unittest.main()
