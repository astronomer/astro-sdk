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

from astro import dataframe as df
from astro import sql as aql
from astro.sql.table import Table

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
            output_table=Table("snowflake_decorator_test", conn_id="snowflake_conn"),
        ).operator.execute({"run_id": "foo"})

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
        @df
        def my_df_func(df: pandas.DataFrame):
            return df.actor_id.count()

        res = self.create_and_run_task(
            my_df_func,
            (),
            {"df": Table("actor", conn_id="postgres_conn", database="pagila")},
        )
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 200
        )

    def test_dataframe_from_sql_basic_op_arg(self):
        @df(conn_id="postgres_conn", database="pagila")
        def my_df_func(df: pandas.DataFrame):
            return df.actor_id.count()

        res = self.create_and_run_task(
            my_df_func,
            (Table("actor", conn_id="postgres_conn", database="pagila"),),
            {},
        )
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 200
        )

    def test_dataframe_from_sql_basic_op_arg_and_kwarg(self):
        @df(conn_id="postgres_conn", database="pagila")
        def my_df_func(actor_df: pandas.DataFrame, film_df: pandas.DataFrame):
            return actor_df.actor_id.count() + film_df.film_id.count()

        res = self.create_and_run_task(
            my_df_func,
            (Table("actor", conn_id="postgres_conn", database="pagila"),),
            {"film_df": Table("film", conn_id="postgres_conn", database="pagila")},
        )
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 1200
        )

    def test_snow_dataframe_from_sql_basic(self):
        @df
        def my_df_func(df: pandas.DataFrame):
            return df.LIVING.count()

        res = self.create_and_run_task(
            my_df_func,
            (),
            {"df": Table("snowflake_decorator_test", conn_id="snowflake_conn")},
        )
        assert (
            XCom.get_one(
                execution_date=DEFAULT_DATE, key=res.key, task_id=res.operator.task_id
            )
            == 47
        )
