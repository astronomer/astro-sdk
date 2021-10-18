"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AIRFLOW__SQL_DECORATOR__CONN_AWS_DEFAULT=aws://KEY:SECRET@ \
    python3 -m unittest tests.operators.test_postgres_operator.TestPostgresOperator.test_load_s3_to_sql_db

"""

import logging
import pathlib
import time
import unittest.mock
from unittest import mock

import pandas as pd
import pytest
from airflow.models import DAG, Connection, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
import astronomer_sql_decorator.sql as aql
from astronomer_sql_decorator.sql.types import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


class TestPostgresAppend(unittest.TestCase):
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

    def test_append(self):
        MAIN_TABLE_NAME = "test_main"
        APPEND_TABLE_NAME = "test_append"
        hook = PostgresHook(postgres_conn_id="postgres_conn", schema="postgres")

        drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table_name=MAIN_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table_name=APPEND_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            foo = aql.append(
                conn_id="postgres_conn",
                database="postgres",
                append_table=APPEND_TABLE_NAME,
                columns=["Sell", "Living"],
                main_table=MAIN_TABLE_NAME,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, MAIN_TABLE_NAME)
        self.wait_for_task_finish(dr, APPEND_TABLE_NAME)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)

        df = pd.read_sql(f"SELECT * FROM {MAIN_TABLE_NAME}", con=hook.get_conn())

        assert len(df) == 6
        assert not df["Sell"].hasnans
        assert df["Rooms"].hasnans

    def test_append_all_fields(self):
        MAIN_TABLE_NAME = "test_main"
        APPEND_TABLE_NAME = "test_append"
        hook = PostgresHook(postgres_conn_id="postgres_conn", schema="postgres")

        drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table_name=MAIN_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table_name=APPEND_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            foo = aql.append(
                conn_id="postgres_conn",
                database="postgres",
                append_table=APPEND_TABLE_NAME,
                main_table=MAIN_TABLE_NAME,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, MAIN_TABLE_NAME)
        self.wait_for_task_finish(dr, APPEND_TABLE_NAME)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)
        df = pd.read_sql(f"SELECT * FROM {MAIN_TABLE_NAME}", con=hook.get_conn())

        assert len(df) == 6
        assert not df["Sell"].hasnans
        assert not df["Rooms"].hasnans

    def test_append_with_cast(self):
        MAIN_TABLE_NAME = "test_main"
        APPEND_TABLE_NAME = "test_append"
        hook = PostgresHook(postgres_conn_id="postgres_conn", schema="postgres")

        drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table_name=MAIN_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table_name=APPEND_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            foo = aql.append(
                conn_id="postgres_conn",
                database="postgres",
                append_table=APPEND_TABLE_NAME,
                columns=["Sell", "Living"],
                casted_columns={"Age": "INTEGER"},
                main_table=MAIN_TABLE_NAME,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, MAIN_TABLE_NAME)
        self.wait_for_task_finish(dr, APPEND_TABLE_NAME)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)
        df = pd.read_sql(f"SELECT * FROM {MAIN_TABLE_NAME}", con=hook.get_conn())

        assert len(df) == 6
        assert not df["Sell"].hasnans
        assert df["Rooms"].hasnans

    def wait_for_task_finish(self, dr, task_id):
        task = dr.get_task_instance(task_id)
        while task.state not in ["success", "failed"]:
            time.sleep(1)
            task = dr.get_task_instance(task_id)

    def test_append_only_cast(self):
        MAIN_TABLE_NAME = "test_main"
        APPEND_TABLE_NAME = "test_append"
        hook = PostgresHook(postgres_conn_id="postgres_conn", schema="postgres")

        drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table_name=MAIN_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table_name=APPEND_TABLE_NAME,
                output_conn_id="postgres_conn",
            )
            foo = aql.append(
                conn_id="postgres_conn",
                database="postgres",
                append_table=APPEND_TABLE_NAME,
                casted_columns={"Age": "INTEGER"},
                main_table=MAIN_TABLE_NAME,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, MAIN_TABLE_NAME)
        self.wait_for_task_finish(dr, APPEND_TABLE_NAME)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)

        df = pd.read_sql(f"SELECT * FROM {MAIN_TABLE_NAME}", con=hook.get_conn())

        assert len(df) == 6
        assert not df["Age"].hasnans
        assert df["Sell"].hasnans
