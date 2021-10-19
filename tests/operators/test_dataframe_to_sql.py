import logging
import os
import unittest.mock
from unittest import mock

import pandas as pd
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.models.xcom import XCom
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

import astronomer_sql_decorator.dataframe as adf

# Import Operator

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


# Mock the `conn_sample` Airflow connection
def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


@mock.patch.dict("os.environ", AIRFLOW__CORE__ENABLE_XCOM_PICKLING="True")
class TestDataframeToSQL(unittest.TestCase):
    """
    Test Sample Operator.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

    def setUp(self):
        super().setUp()
        self.clear_run()
        self.addCleanup(self.clear_run)
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )

    def clear_run(self):
        self.run = False

    def tearDown(self):
        super().tearDown()
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def create_and_run_task(self, decorator_func, op_args, op_kwargs):
        with self.dag:
            f = decorator_func(*op_args, **op_kwargs)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        return f

    def test_dataframe_to_sql_basic_op_arg(self):
        @adf.to_sql(conn_id="postgres_conn", database="pagila", output_table_name="foo")
        def my_df_func():
            return pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]})

        res = self.create_and_run_task(my_df_func, (), {})
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == "foo"
        )
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_conn", schema="pagila")

        df = pd.read_sql(f"SELECT * FROM foo", con=hook.get_conn())
        assert df.equals(pd.DataFrame(data={"col1": [1, 2], "col2": [3, 4]}))

    def test_snow_dataframe_to_sql_basic(self):
        hook = SnowflakeHook(
            snowflake_conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
        )

        hook.run("DROP TABLE IF EXISTS TEST_DF_LOAD")

        @adf.to_sql(
            conn_id="snowflake_conn",
            schema="SANDBOX_DANIEL",
            database="DWH_LEGACY",
            warehouse="TRANSFORMING_DEV",
            output_table_name="TEST_DF_LOAD",
        )
        def my_df_func():
            return pd.DataFrame(data={"COL1": [1, 2], "COL2": [3, 4]})

        res = self.create_and_run_task(my_df_func, (), {})
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == "TEST_DF_LOAD"
        )

        df = pd.read_sql(f"SELECT * FROM TEST_DF_LOAD", con=hook.get_conn())
        assert df.equals(pd.DataFrame(data={"COL1": [1, 2], "COL2": [3, 4]}))
