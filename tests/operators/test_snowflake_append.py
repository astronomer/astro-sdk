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

from airflow.executors.debug_executor import DebugExecutor
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State

# Import Operator
from astro import sql as aql
from astro.sql.table import Table
from tests.operators import utils as test_utils

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
        cwd = pathlib.Path(__file__).parent
        cls.TABLE_1_NAME = test_utils.get_table_name("TEST_APPEND_1")
        cls.TABLE_2_NAME = test_utils.get_table_name("TEST_APPEND_2")

        cls.TABLE_1 = Table(
            table_name=cls.TABLE_1_NAME,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        cls.TABLE_2 = Table(
            table_name=cls.TABLE_2_NAME,
            database=os.getenv("SNOWFLAKE_DATABASE"),
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

        cls.load_main = aql.load_file(
            path=str(cwd) + "/../data/homes_main.csv",
            file_conn_id="",
            output_table=cls.TABLE_1,
        )
        cls.load_main.operator.execute({"run_id": "foo"})

        cls.load_append = aql.load_file(
            path=str(cwd) + "/../data/homes_append.csv",
            file_conn_id="",
            output_table=cls.TABLE_2,
        )
        cls.load_append.operator.execute({"run_id": "foo"})

    @classmethod
    def tearDownClass(cls):
        test_utils.drop_table_snowflake(
            table_name=cls.TABLE_1_NAME,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        test_utils.drop_table_snowflake(
            table_name=cls.TABLE_2_NAME,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

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

    def run_append_func(self, columns, casted_columns):
        with self.dag:
            foo = aql.append(
                append_table=self.TABLE_2,
                columns=columns,
                casted_columns=casted_columns,
                main_table=self.TABLE_1,
            )
        test_utils.run_dag(self.dag)

    def test_append(self):
        hook = get_snowflake_hook()
        previous_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_main.operator.output_table.qualified_name()}"
        )
        append_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_append.operator.output_table.qualified_name()}"
        )
        self.run_append_func([], {})
        main_table_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_main.operator.output_table.qualified_name()}"
        )
        assert (
            main_table_count[0]["COUNT(*)"]
            == previous_count[0]["COUNT(*)"] + append_count[0]["COUNT(*)"]
        )

    def test_append_no_cast(self):
        hook = get_snowflake_hook()
        previous_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_main.operator.output_table.qualified_name()}"
        )
        append_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_append.operator.output_table.qualified_name()}"
        )
        self.run_append_func(["BEDS", "ACRES"], {})

        df = hook.get_pandas_df(
            f"SELECT * FROM {self.load_main.operator.output_table.qualified_name()}"
        )

        assert len(df) == previous_count[0]["COUNT(*)"] + append_count[0]["COUNT(*)"]
        assert not df["BEDS"].hasnans
        assert df["ROOMS"].hasnans

    def test_append_with_cast(self):
        hook = get_snowflake_hook()
        previous_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_main.operator.output_table.qualified_name()}"
        )
        append_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_append.operator.output_table.qualified_name()}"
        )
        self.run_append_func(["beds"], {"acres": "FLOAT"})

        df = hook.get_pandas_df(
            f"SELECT * FROM {self.load_main.operator.output_table.qualified_name()}"
        )

        assert len(df) == previous_count[0]["COUNT(*)"] + append_count[0]["COUNT(*)"]
        assert not df["ACRES"].hasnans
        assert df["LIVING"].hasnans

    def test_append_with_cast_and_no_cast(self):
        hook = get_snowflake_hook()
        previous_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_main.operator.output_table.qualified_name()}"
        )
        append_count = hook.run(
            f"SELECT COUNT(*) FROM {self.load_append.operator.output_table.qualified_name()}"
        )

        self.run_append_func(
            ["beds"],
            {"acres": "FLOAT"},
        )

        df = hook.get_pandas_df(
            f"SELECT * FROM {self.load_main.operator.output_table.qualified_name()}"
        )

        assert len(df) == previous_count[0]["COUNT(*)"] + append_count[0]["COUNT(*)"]
        assert not df["BEDS"].hasnans
        assert not df["ACRES"].hasnans
        assert df["LIVING"].hasnans


if __name__ == "__main__":
    unittest.main()
