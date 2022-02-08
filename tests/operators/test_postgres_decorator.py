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
    python3 -m unittest tests.operators.test_postgres_decorator.TestPostgresDecorator.test_postgres

"""

import logging
import pathlib
import unittest.mock
from unittest import mock

import pandas as pd
import pytest
from airflow.executors.debug_executor import DebugExecutor
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
import astro.sql as aql
from astro import dataframe as adf
from astro.sql.table import Table, TempTable
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
import time


def wait_for_task_finish(dr, task_id):
    task = dr.get_task_instance(task_id)
    while task.state not in ["success", "failed"]:
        time.sleep(1)
        task = dr.get_task_instance(task_id)


def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


@mock.patch.dict("os.environ", AIRFLOW__CORE__ENABLE_XCOM_PICKLING="True")
class TestPostgresDecorator(unittest.TestCase):
    """Test Postgres Decorator."""

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
        test_utils.run_dag(self.dag)
        return f

    def test_dataframe_to_postgres(self):
        @adf
        def get_dataframe():
            return pd.DataFrame(
                {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
            )

        @aql.transform
        def sample_pg(input_table: Table):
            return "SELECT * FROM {input_table}"

        with self.dag:
            my_df = get_dataframe(
                output_table=Table(
                    table_name="my_df_table", conn_id="postgres_conn", database="pagila"
                )
            )
            pg_df = sample_pg(my_df)
        self.dag.clear(
            start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, dag_run_state=State.NONE
        )

        self.dag.run(
            executor=DebugExecutor(),
            start_date=timezone.utcnow(),
            run_at_least_once=True,
        )

    def test_with_invalid_dag_name(self):
        self.dag.dag_id = "my=dag"
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        drop_table(
            table_name='tmp_astro."my=dag_sample_pg_1"',
            postgres_conn=self.hook_target.get_conn(),
        )

        @aql.transform()
        def sample_pg(input_table: Table):
            return "SELECT * FROM {input_table} WHERE last_name LIKE 'G%%'"

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "input_table": Table(
                    table_name="actor",
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
                ),
            },
        )
        df = pd.read_sql(
            f'SELECT * FROM tmp_astro."my=dag_sample_pg_1"',
            con=self.hook_target.get_conn(),
        )
        assert df.iloc[0].to_dict()["first_name"] == "PENELOPE"

    def test_dataframe_to_postgres_kwarg(self):
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        @adf
        def get_dataframe():
            return pd.DataFrame(
                {"numbers": [1, 2, 3], "colors": ["red", "white", "blue"]}
            )

        @aql.transform
        def sample_pg(input_table: Table):
            return "SELECT * FROM {input_table}"

        with self.dag:
            my_df = get_dataframe(
                output_table=TempTable(conn_id="postgres_conn", database="pagila")
            )
            pg_df = sample_pg(input_table=my_df)

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        my_df.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        wait_for_task_finish(dr, my_df.operator.task_id)
        pg_df.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        wait_for_task_finish(dr, pg_df.operator.task_id)
        df = pd.read_sql(
            f"SELECT * FROM tmp_astro.test_dag_sample_pg_1",
            con=self.hook_target.get_conn(),
        )
        assert df.iloc[0].to_dict()["colors"] == "red"

    def test_postgres(self):
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        @aql.transform
        def sample_pg(input_table: Table):
            return "SELECT * FROM {input_table} WHERE last_name LIKE 'G%%'"

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "input_table": Table(
                    table_name="actor",
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
                ),
            },
        )
        df = pd.read_sql(
            f"SELECT * FROM tmp_astro.test_dag_sample_pg_1",
            con=self.hook_target.get_conn(),
        )
        assert df.iloc[0].to_dict()["first_name"] == "PENELOPE"

    def test_postgres_with_semicolon(self):
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        @aql.transform
        def sample_pg(input_table: Table):
            # Add trailing whitespaces to ensure it can still catch the semicolon
            return "SELECT * FROM {input_table} WHERE last_name LIKE 'G%%';   " "    "

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "input_table": Table(
                    table_name="actor",
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
                ),
            },
        )
        df = pd.read_sql(
            f"SELECT * FROM tmp_astro.test_dag_sample_pg_1",
            con=self.hook_target.get_conn(),
        )
        assert df.iloc[0].to_dict()["first_name"] == "PENELOPE"

    def test_postgres_with_parameter(self):
        @aql.transform(conn_id="postgres_conn", database="pagila")
        def sample_pg(input_table: Table):
            return "SELECT * FROM {input_table} WHERE last_name LIKE {last_name}", {
                "last_name": "G%%"
            }

        self.create_and_run_task(
            sample_pg, (), {"input_table": Table(table_name="actor", schema="public")}
        )

    def test_postgres_with_jinja_template(self):
        @aql.transform()
        def sample_pg(input_table: Table):
            return (
                "SELECT * FROM {input_table} WHERE rental_date < '{{ execution_date }}'"
            )

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "input_table": Table(
                    table_name="rental",
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
                )
            },
        )

    def test_postgres_with_jinja_template_params(self):
        @aql.transform(conn_id="postgres_conn", database="pagila")
        def sample_pg(input_table: Table):
            return "SELECT * FROM {input_table} WHERE rental_date < {r_date}", {
                "r_date": "{{ execution_date }}"
            }

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "input_table": Table(
                    table_name="rental",
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
                )
            },
        )

    def test_postgres_join(self):
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        drop_table(table_name="my_table", postgres_conn=self.hook_target.get_conn())

        @aql.transform
        def sample_pg(actor: Table, film_actor_join: Table, unsafe_parameter):
            return (
                "SELECT {actor}.actor_id, first_name, last_name, COUNT(film_id) "
                "FROM {actor} JOIN {film_actor_join} ON {actor}.actor_id = {film_actor_join}.actor_id "
                "WHERE last_name LIKE {unsafe_parameter} GROUP BY {actor}.actor_id"
            )

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "actor": Table(
                    table_name="actor",
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
                ),
                "film_actor_join": Table(table_name="film_actor", schema="public"),
                "unsafe_parameter": "G%%",
                "output_table": Table("my_table"),
            },
        )
        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM tmp_astro.my_table", con=self.hook_target.get_conn()
        )
        assert df.iloc[0].to_dict() == {
            "actor_id": 191,
            "first_name": "GREGORY",
            "last_name": "GOODING",
            "count": 30,
        }

        drop_table(table_name="my_table", postgres_conn=self.hook_target.get_conn())

    def test_sql_file(self):
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        drop_table(
            table_name="my_table_from_file", postgres_conn=self.hook_target.get_conn()
        )

        cwd = pathlib.Path(__file__).parent

        with self.dag:
            f = aql.transform_file(
                sql=str(cwd) + "/test.sql",
                conn_id="postgres_conn",
                schema="public",
                database="pagila",
                parameters={
                    "actor": Table("actor", schema="public"),
                    "film_actor_join": Table("film_actor", schema="public"),
                    "unsafe_parameter": "G%%",
                },
                output_table=Table("my_table_from_file"),
            )

        test_utils.run_dag(self.dag)

        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM tmp_astro.my_table_from_file",
            con=self.hook_target.get_conn(),
        )
        assert df.iloc[0].to_dict() == {
            "actor_id": 191,
            "first_name": "GREGORY",
            "last_name": "GOODING",
            "count": 30,
        }

        drop_table(table_name="my_table", postgres_conn=self.hook_target.get_conn())

    def test_raw_sql(self):
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )
        drop_table(
            table_name="my_raw_sql_table", postgres_conn=self.hook_target.get_conn()
        )

        @aql.run_raw_sql
        def sample_pg(
            actor: Table, film_actor_join: Table, output_table_name, unsafe_parameter
        ):
            return (
                "CREATE TABLE my_raw_sql_table AS (SELECT {actor}.actor_id, first_name, last_name, COUNT(film_id) "
                "FROM {actor} JOIN {film_actor_join} ON {actor}.actor_id = {film_actor_join}.actor_id "
                "WHERE last_name LIKE {unsafe_parameter} GROUP BY {actor}.actor_id)"
            )

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "actor": Table(
                    table_name="actor",
                    conn_id="postgres_conn",
                    database="pagila",
                    schema="public",
                ),
                "film_actor_join": Table(table_name="film_actor", schema="public"),
                "unsafe_parameter": "G%%",
                "output_table_name": "my_table",
            },
        )
        # Read table from db
        df = pd.read_sql(
            f"SELECT * FROM my_raw_sql_table", con=self.hook_target.get_conn()
        )
        assert df.iloc[0].to_dict() == {
            "actor_id": 191,
            "first_name": "GREGORY",
            "last_name": "GOODING",
            "count": 30,
        }

        drop_table(
            table_name="my_raw_sql_table", postgres_conn=self.hook_target.get_conn()
        )
