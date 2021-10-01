"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_sample_operator.TestSampleOperator

"""

import json
import logging
import os
import unittest.mock
from unittest import mock

import requests_mock
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from pandas import DataFrame

# Import Operator
from astronomer_sql_decorator import sql as aql
from astronomer_sql_decorator.sql.types import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, snowflake_conn):
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


# Mock the `conn_sample` Airflow connection
@mock.patch.dict(
    "os.environ", AIRFLOW_CONN_CONN_SAMPLE="http://https%3A%2F%2Fwww.httpbin.org%2F"
)
class TestSampleOperator(unittest.TestCase):
    """
    Test Sample Operator.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            session.query(Connection).delete()
            snowflake_connection = Connection(
                conn_id="snowflake_conn",
                conn_type="snowflake",
                host="https://gp21411.us-east-1.snowflakecomputing.com",
                login=os.environ["SNOW_ACCOUNT_NAME"],
                port=443,
                password=os.environ["SNOW_PASSWORD"],
                extra={
                    "account": "gp21411",
                    "region": "us-east-1",
                    "role": "TRANSFORMER",
                },
            )
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(Connection).delete()
            session.add(snowflake_connection)

    def setUp(self):
        super().setUp()
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )
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

    def test_snowflake_query(self):
        @aql.transform(
            conn_id="snowflake_conn",
            warehouse="TRANSFORMING_DEV",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
        )
        def sample_snow(input_table: Table, output_table_name):
            return "SELECT * FROM {input_table} LIMIT 10"

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            warehouse="TRANSFORMING_DEV",
        )

        drop_table(
            snowflake_conn=hook.get_conn(),
            table_name='"DWH_LEGACY"."SANDBOX_DANIEL"."SNOWFLAKE_TRANSFORM_TEST_TABLE"',
        )
        with self.dag:
            f = sample_snow(
                input_table="PRICE_TABLE",
                output_table_name="SNOWFLAKE_TRANSFORM_TEST_TABLE",
            )

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        df = hook.get_pandas_df(
            'SELECT * FROM "DWH_LEGACY"."SANDBOX_DANIEL"."SNOWFLAKE_TRANSFORM_TEST_TABLE"'
        )
        assert len(df) == 10

    def test_raw_sql(self):
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            warehouse="TRANSFORMING_DEV",
        )

        drop_table(
            snowflake_conn=hook.get_conn(),
            table_name='"DWH_LEGACY"."SANDBOX_DANIEL"."SNOWFLAKE_TRANSFORM_RAW_SQL_TEST_TABLE"',
        )

        @aql.run_raw_sql(
            conn_id="snowflake_conn",
            warehouse="TRANSFORMING_DEV",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
        )
        def sample_snow(my_input_table: Table):
            return "CREATE TABLE SNOWFLAKE_TRANSFORM_RAW_SQL_TEST_TABLE AS (SELECT * FROM {my_input_table} LIMIT 5)"

        with self.dag:
            f = sample_snow(
                my_input_table="PRICE_TABLE",
            )

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

        # Read table from db
        df = hook.get_pandas_df(
            'SELECT * FROM "DWH_LEGACY"."SANDBOX_DANIEL"."SNOWFLAKE_TRANSFORM_RAW_SQL_TEST_TABLE"'
        )
        assert len(df) == 5


if __name__ == "__main__":
    unittest.main()
