import logging
import os
import pathlib
import unittest.mock
from unittest import mock

import pandas
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.models.xcom import XCom
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

import astronomer_sql_decorator.dataframe as adf
from astronomer_sql_decorator import sql as aql

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
class TestDataframeFromSQL(unittest.TestCase):
    """
    Test Dataframe From SQL
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
        cwd = pathlib.Path(__file__).parent
        aql.load_file(
            path=str(cwd) + "/../data/homes.csv",
            output_conn_id="snowflake_conn",
            output_table_name="snowflake_decorator_test",
        ).operator.execute(None)

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

    def test_dataframe_from_sql_basic(self):
        @adf.from_sql(conn_id="postgres_conn", database="pagila")
        def my_df_func(df: pandas.DataFrame):
            return df.actor_id.count()

        res = self.create_and_run_task(my_df_func, (), {"df": "actor"})
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 200
        )

    def test_dataframe_from_sql_basic_op_arg(self):
        @adf.from_sql(conn_id="postgres_conn", database="pagila")
        def my_df_func(df: pandas.DataFrame):
            return df.actor_id.count()

        res = self.create_and_run_task(my_df_func, ("actor",), {})
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 200
        )

    def test_dataframe_from_sql_basic_op_arg_and_kwarg(self):
        @adf.from_sql(conn_id="postgres_conn", database="pagila")
        def my_df_func(actor_df: pandas.DataFrame, film_df: pandas.DataFrame):
            return actor_df.actor_id.count() + film_df.film_id.count()

        res = self.create_and_run_task(my_df_func, ("actor",), {"film_df": "film"})
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 1200
        )

    def test_snow_dataframe_from_sql_basic(self):
        @adf.from_sql(
            conn_id="snowflake_conn",
        )
        def my_df_func(df: pandas.DataFrame):
            return df.LIVING.count()

        res = self.create_and_run_task(
            my_df_func, (), {"df": "snowflake_decorator_test"}
        )
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 47
        )
