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

    python3 -m unittest tests.operators.test_snowflake_append.TestSnowflakeAppend.test_append

"""

import logging
import os
import pathlib
import unittest.mock

from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
from astro import sql as aql
from astro.sql.table import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, snowflake_conn):
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


def get_snowflake_hook():
    hook = SnowflakeHook(
        snowflake_conn_id="snowflake_conn",
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )
    return hook


class TestSnowflakeAppend(unittest.TestCase):
    """Test Snowflake Append."""

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

    def run_append_func(self, columns, casted_columns):
        cwd = pathlib.Path(__file__).parent

        with self.dag:
            load_main = aql.load_file(
                path=str(cwd) + "/../data/homes_main.csv",
                output_table=Table(
                    table_name="test_append_1", conn_id="snowflake_conn"
                ),
            )
            load_append = aql.load_file(
                path=str(cwd) + "/../data/homes_append.csv",
                output_table=Table(
                    table_name="test_append_2", conn_id="snowflake_conn"
                ),
            )
            foo = aql.append(
                conn_id="snowflake_conn",
                append_table=load_append,
                columns=columns,
                casted_columns=casted_columns,
                main_table=load_main,
            )

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )

        load_main.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        load_append.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.load_main = load_main
        self.wait_for_task_finish(dr, load_main.operator.task_id)
        self.wait_for_task_finish(dr, load_append.operator.task_id)

        foo.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        self.wait_for_task_finish(dr, foo.task_id)

    def test_append(self):
        hook = get_snowflake_hook()

        self.run_append_func([], {})
        main_table_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_main.operator.output_table.qualified_name()}"
        )
        assert main_table_count[0]["COUNT(*)"] == 6

    def test_append_no_cast(self):
        hook = get_snowflake_hook()

        self.run_append_func(["BEDS"], {})

        df = hook.get_pandas_df(
            f"SELECT * FROM {self.load_main.operator.output_table.qualified_name()}"
        )

        assert len(df) == 6
        assert not df["BEDS"].hasnans
        assert df["ROOMS"].hasnans

    def test_append_with_cast(self):
        self.run_append_func([], {"ACRES": "FLOAT"})

        hook = get_snowflake_hook()

        df = hook.get_pandas_df(
            f"SELECT * FROM {self.load_main.operator.output_table.qualified_name()}"
        )

        assert len(df) == 6
        assert not df["ACRES"].hasnans
        assert df["BEDS"].hasnans

    def test_append_with_cast_and_no_cast(self):
        hook = get_snowflake_hook()

        self.run_append_func(
            ["BEDS"],
            {"ACRES": "FLOAT"},
        )

        df = hook.get_pandas_df(
            f"SELECT * FROM {self.load_main.operator.output_table.qualified_name()}"
        )

        assert len(df) == 6
        assert not df["BEDS"].hasnans
        assert not df["ACRES"].hasnans
        assert df["LIVING"].hasnans


if __name__ == "__main__":
    unittest.main()
