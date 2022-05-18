"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AWS_ACCESS_KEY_ID=KEY \
    AWS_SECRET_ACCESS_KEY=SECRET \
    python3 -m unittest tests.operators.test_postgres_operator.TestPostgresOperator.test_load_s3_to_sql_db

"""

import logging
import pathlib
import time
import unittest

import pandas as pd
from airflow.executors.debug_executor import DebugExecutor
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

import astro.sql as aql

# Import Operator
from astro.databases import create_database
from astro.sql.table import Table
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
MAIN_TABLE_NAME = "test_main"
APPEND_TABLE_NAME = "test_append"
CWD = pathlib.Path(__file__).parent


def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name};")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


def run_dag(dag):
    dag.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, dag_run_state=State.NONE)

    dag.run(
        executor=DebugExecutor(),
        start_date=DEFAULT_DATE,
        end_date=DEFAULT_DATE,
        run_at_least_once=True,
    )


class TestSQLiteAppend(unittest.TestCase):
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
        self.MAIN_TABLE_NAME = "test_main"
        self.APPEND_TABLE_NAME = "test_append"
        self.main_table = Table(
            name=self.MAIN_TABLE_NAME,
            conn_id="sqlite_conn",
        )
        self.append_table = Table(
            name=self.APPEND_TABLE_NAME,
            conn_id="sqlite_conn",
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
            run_dag(self.dag)
        return f

    def wait_for_task_finish(self, dr, task_id):
        task = dr.get_task_instance(task_id)
        while task.state not in ["success", "failed"]:
            time.sleep(1)
            task = dr.get_task_instance(task_id)

    def test_append(self):
        hook = SqliteHook(sqlite_conn_id="sqlite_conn")

        drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table=self.main_table,
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table=self.append_table,
            )
            aql.append(
                columns=["sell", "living"],
                main_table=load_main,
                append_table=load_append,
            )
        test_utils.run_dag(self.dag)

        database = create_database(load_main.operator.output_table.conn_id)
        qualified_name = database.get_table_qualified_name(
            load_main.operator.output_table
        )
        df = pd.read_sql(
            f"SELECT * FROM {qualified_name}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["sell"].hasnans
        assert df["rooms"].hasnans

    def test_append_all_fields(self):
        hook = SqliteHook(sqlite_conn_id="sqlite_conn")

        drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table=self.main_table,
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table=self.append_table,
            )
            aql.append(
                main_table=load_main,
                append_table=load_append,
            )
        test_utils.run_dag(self.dag)

        database = create_database(load_main.operator.output_table.conn_id)
        qualified_name = database.get_table_qualified_name(
            load_main.operator.output_table
        )
        df = pd.read_sql(
            f"SELECT * FROM {qualified_name}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["sell"].hasnans
        assert not df["rooms"].hasnans

    def test_append_with_cast(self):
        hook = SqliteHook(sqlite_conn_id="sqlite_conn")

        # drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        # drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table=self.main_table,
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table=self.append_table,
            )
            aql.append(
                columns=["sell", "living"],
                casted_columns={"age": "INTEGER"},
                main_table=load_main,
                append_table=load_append,
            )
        test_utils.run_dag(self.dag)
        database = create_database(load_main.operator.output_table.conn_id)
        qualified_name = database.get_table_qualified_name(
            load_main.operator.output_table
        )
        df = pd.read_sql(
            f"SELECT * FROM {qualified_name}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["sell"].hasnans
        assert df["rooms"].hasnans

    def test_append_only_cast(self):
        hook = SqliteHook(sqlite_conn_id="sqlite_conn")

        drop_table(table_name="test_main", postgres_conn=hook.get_conn())
        drop_table(table_name="test_append", postgres_conn=hook.get_conn())

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table=self.main_table,
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table=self.append_table,
            )
            aql.append(
                casted_columns={"age": "INTEGER"},
                main_table=load_main,
                append_table=load_append,
            )
        test_utils.run_dag(self.dag)

        database = create_database(load_main.operator.output_table.conn_id)
        qualified_name = database.get_table_qualified_name(
            load_main.operator.output_table
        )
        df = pd.read_sql(
            f"SELECT * FROM {qualified_name}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["age"].hasnans
        assert df["sell"].hasnans
