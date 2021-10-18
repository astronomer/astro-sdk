"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_snowflake_decorator.TestSnowflakeOperator

"""

import logging
import unittest.mock
from unittest import mock

import pytest
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

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


class TestSnowflakeOperator(unittest.TestCase):
    """
    Test Sample Operator.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

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

    def wait_for_task_finish(self, dr, task_id):
        import time

        task = dr.get_task_instance(task_id)
        while task.state not in ["success", "failed"]:
            time.sleep(1)
            task = dr.get_task_instance(task_id)

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
            database="DWH_LEGACY",
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

    def run_append_func(
        self, main_table_name, append_table_name, columns, casted_columns
    ):
        MAIN_TABLE_NAME = main_table_name
        APPEND_TABLE_NAME = append_table_name
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
        )

        hook.run(
            'DROP TABLE IF EXISTS TEST_APPEND1; CREATE TABLE TEST_APPEND1 AS (SELECT * FROM "DWH_LEGACY"."SANDBOX_DANIEL"."PRICE_TABLE" WHERE BILL_AMOUNT < 300000)'
        )
        hook.run(
            'DROP TABLE IF EXISTS TEST_APPEND2; CREATE TABLE TEST_APPEND2 AS (SELECT * FROM "DWH_LEGACY"."SANDBOX_DANIEL"."PRICE_TABLE" WHERE BILL_AMOUNT > 300000)'
        )

        drop_table(table_name="test_main", snowflake_conn=hook.get_conn())
        drop_table(table_name="test_append", snowflake_conn=hook.get_conn())

        with self.dag:
            foo = aql.append(
                conn_id="snowflake_conn",
                warehouse="TRANSFORMING_DEV",
                schema="SANDBOX_DANIEL",
                database="DWH_LEGACY",
                append_table=APPEND_TABLE_NAME,
                columns=columns,
                casted_columns=casted_columns,
                main_table=MAIN_TABLE_NAME,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        foo.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, "append_func")

    def test_append(self):
        MAIN_TABLE_NAME = "TEST_APPEND1"
        APPEND_TABLE_NAME = "TEST_APPEND2"
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
        )
        self.run_append_func(MAIN_TABLE_NAME, APPEND_TABLE_NAME, [], {})
        main_table_count = hook.run(f"SELECT COUNT(*) FROM {MAIN_TABLE_NAME}")
        original_table_count = hook.run(
            'SELECT COUNT(*) FROM "DWH_LEGACY"."SANDBOX_DANIEL"."PRICE_TABLE"'
        )
        assert main_table_count[0]["COUNT(*)"] == original_table_count[0]["COUNT(*)"]

    def test_append_no_cast(self):
        MAIN_TABLE_NAME = "TEST_APPEND1"
        APPEND_TABLE_NAME = "TEST_APPEND2"
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
        )

        self.run_append_func(
            MAIN_TABLE_NAME, APPEND_TABLE_NAME, ["STRIPECUSTOMERID"], {}
        )

        df = hook.get_pandas_df(f"SELECT * FROM {MAIN_TABLE_NAME}")

        assert len(df) == 70
        assert not df["STRIPECUSTOMERID"].hasnans
        assert df["WORKSPACE"].hasnans

    def test_append_with_cast(self):
        MAIN_TABLE_NAME = "TEST_APPEND1"
        APPEND_TABLE_NAME = "TEST_APPEND2"

        self.run_append_func(
            MAIN_TABLE_NAME, APPEND_TABLE_NAME, [], {"BILL_AMOUNT": "FLOAT"}
        )

        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
        )
        df = hook.get_pandas_df(f"SELECT * FROM {MAIN_TABLE_NAME}")

        assert len(df) == 70
        assert not df["BILL_AMOUNT"].hasnans
        assert df["WORKSPACE"].hasnans

    def test_append_with_cast_and_no_cast(self):
        MAIN_TABLE_NAME = "TEST_APPEND1"
        APPEND_TABLE_NAME = "TEST_APPEND2"
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
        )
        self.run_append_func(
            MAIN_TABLE_NAME,
            APPEND_TABLE_NAME,
            ["STRIPECUSTOMERID"],
            {"BILL_AMOUNT": "FLOAT"},
        )

        df = hook.get_pandas_df(f"SELECT * FROM {MAIN_TABLE_NAME}")

        assert len(df) == 70
        assert not df["BILL_AMOUNT"].hasnans
        assert not df["STRIPECUSTOMERID"].hasnans
        assert df["WORKSPACE"].hasnans


if __name__ == "__main__":
    unittest.main()
