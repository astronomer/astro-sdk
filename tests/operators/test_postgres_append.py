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
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    AIRFLOW__ASTRO__CONN_AWS_DEFAULT=aws://KEY:SECRET@ \
    python3 -m unittest tests.operators.test_postgres_operator.TestPostgresOperator.test_load_s3_to_sql_db

"""

import logging
import pathlib
import time
import unittest.mock

import pandas as pd
from airflow.executors.debug_executor import DebugExecutor
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from google.cloud import bigquery

# Import Operator
import astro.sql as aql
from astro.sql.operators.temp_hooks import TempPostgresHook
from astro.sql.table import Table

# from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
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
        self.MAIN_TABLE_NAME = "test_main"
        self.APPEND_TABLE_NAME = "test_append"
        self.main_table = Table(
            table_name=self.MAIN_TABLE_NAME,
            conn_id="postgres_sqla_conn",
            database="pagila",
            schema="public",
        )
        self.append_table = Table(
            table_name=self.APPEND_TABLE_NAME,
            conn_id="postgres_sqla_conn",
            database="pagila",
            schema="public",
        )

        self.main_table_bigquery = Table(
            table_name=self.MAIN_TABLE_NAME, conn_id="bigquery", schema="tmp_astro"
        )
        self.append_table_bigquery = Table(
            table_name=self.APPEND_TABLE_NAME, conn_id="bigquery", schema="tmp_astro"
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
        hook = PostgresHook(postgres_conn_id="postgres_sqla_conn", schema="pagila")

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
            foo = aql.append(
                columns=["sell", "living"],
                main_table=self.main_table,
                append_table=self.append_table,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, load_main.operator.task_id)
        self.wait_for_task_finish(dr, load_append.operator.task_id)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)

        df = pd.read_sql(
            f"SELECT * FROM {load_main.operator.output_table.qualified_name()}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["sell"].hasnans
        assert df["rooms"].hasnans

    def test_append_all_fields(self):

        hook = TempPostgresHook(postgres_conn_id="postgres_sqla_conn", schema="pagila")

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
            foo = aql.append(main_table=self.main_table, append_table=self.append_table)
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        main_table = load_main.operator.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE
        )
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, load_main.operator.task_id)
        self.wait_for_task_finish(dr, load_append.operator.task_id)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)
        df = pd.read_sql(
            f"SELECT * FROM {load_main.operator.output_table.qualified_name()}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["sell"].hasnans
        assert not df["rooms"].hasnans

    def test_append_with_cast(self):
        hook = PostgresHook(postgres_conn_id="postgres_sqla_conn", schema="pagila")

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
            foo = aql.append(
                columns=["sell", "living"],
                casted_columns={"age": "INTEGER"},
                main_table=self.main_table,
                append_table=self.append_table,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, load_main.operator.task_id)
        self.wait_for_task_finish(dr, load_append.operator.task_id)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)
        df = pd.read_sql(
            f"SELECT * FROM {load_main.operator.output_table.qualified_name()}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["sell"].hasnans
        assert df["rooms"].hasnans

    def test_append_only_cast(self):
        MAIN_TABLE_NAME = "test_main"
        APPEND_TABLE_NAME = "test_append"
        hook = PostgresHook(postgres_conn_id="postgres_sqla_conn", schema="pagila")

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
            foo = aql.append(
                casted_columns={"age": "INTEGER"},
                main_table=self.main_table,
                append_table=self.append_table,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, load_main.operator.task_id)
        self.wait_for_task_finish(dr, load_append.operator.task_id)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)

        df = pd.read_sql(
            f"SELECT * FROM {load_main.operator.output_table.qualified_name()}",
            con=hook.get_conn(),
        )

        assert len(df) == 6
        assert not df["age"].hasnans
        assert df["sell"].hasnans

    def test_append_all_fields_bigquery(self):
        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table=self.main_table_bigquery,
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table=self.append_table_bigquery,
            )
            foo = aql.append(
                main_table=self.main_table_bigquery,
                append_table=self.append_table_bigquery,
            )
        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        main_table = load_main.operator.run(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE
        )
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, load_main.operator.task_id)
        self.wait_for_task_finish(dr, load_append.operator.task_id)
        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)

        client = bigquery.Client()
        query_job = client.query(
            f"SELECT * FROM {load_main.operator.output_table.qualified_name()}"
        )
        bigquery_df = query_job.to_dataframe()

        assert len(bigquery_df) == 6
        assert not bigquery_df["sell"].hasnans
        assert not bigquery_df["rooms"].hasnans
