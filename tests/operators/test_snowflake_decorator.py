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

    python3 -m unittest tests.operators.test_snowflake_decorator.TestSnowflakeOperator

"""

import logging
import os
import pathlib
import unittest.mock

import pytest
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone
from airflow.utils.session import create_session

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


class TestSnowflakeOperator(unittest.TestCase):
    """
    Test Sample Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.snowflake_table = test_utils.get_table_name(
            "SNOWFLAKE_TRANSFORM_TEST_TABLE"
        )
        cls.snow_inherit_table = test_utils.get_table_name(
            "SNOWFLAKE_INHERIT_TEST_TABLE"
        )
        cls.snowflake_table_raw_sql = test_utils.get_table_name(
            "SNOWFLAKE_TRANSFORM_RAW_SQL_TEST_TABLE"
        )

    @classmethod
    def tearDownClass(cls):
        test_utils.drop_table_snowflake(
            table_name=cls.snowflake_table,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        test_utils.drop_table_snowflake(
            table_name=cls.snow_inherit_table,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )
        test_utils.drop_table_snowflake(
            table_name=cls.snowflake_table_raw_sql,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

    def setUp(self):
        cwd = pathlib.Path(__file__).parent
        self.input_table_name = test_utils.get_table_name("snowflake_decorator_test")
        aql.load_file(
            path=str(cwd) + "/../data/homes.csv",
            output_table=Table(
                self.input_table_name,
                conn_id="snowflake_conn",
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            ),
        ).operator.execute({"run_id": "foo"})
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

        test_utils.drop_table_snowflake(
            table_name=self.input_table_name,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

    def run_snow_query(self, role=None):
        @aql.transform
        def sample_snow(input_table: Table):
            return "SELECT * FROM {{input_table}} LIMIT 10"

        hook = get_snowflake_hook()
        drop_table(
            snowflake_conn=hook.get_conn(),
            table_name=self.snowflake_table,
        )

        with self.dag:
            f = sample_snow(
                input_table=Table(
                    self.input_table_name,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                    role=role,
                ),
                output_table=Table(
                    self.snowflake_table,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
            )
            x = sample_snow(
                input_table=f,
                output_table=Table(
                    self.snow_inherit_table,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
            )
        test_utils.run_dag(self.dag)

        df = hook.get_pandas_df(
            f'SELECT * FROM "{os.getenv("SNOWFLAKE_DATABASE")}"."{os.getenv("SNOWFLAKE_SCHEMA")}"."{self.snow_inherit_table}"'
        )
        assert len(df) == 10

    def test_snowflake_query(self):
        self.run_snow_query()

    def test_roles_work_failing(self):
        with pytest.raises(Exception):
            self.run_snow_query(role="foo")

    def test_roles_work_passing(self):
        self.run_snow_query(role=os.getenv("SNOWFLAKE_ROLE"))

    def test_raw_sql(self):
        hook = get_snowflake_hook()
        drop_table(
            snowflake_conn=hook.get_conn(),
            table_name=self.snowflake_table_raw_sql,
        )

        @aql.run_raw_sql(
            conn_id="snowflake_conn",
        )
        def sample_snow(
            my_input_table: Table, snowflake_table_raw_sql: Table, num_rows: int
        ):
            return "CREATE TABLE {{snowflake_table_raw_sql}} AS (SELECT * FROM {{my_input_table}} LIMIT {{num_rows}})"

        with self.dag:
            f = sample_snow(
                my_input_table=Table(
                    self.input_table_name,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                snowflake_table_raw_sql=Table(
                    self.snowflake_table_raw_sql,
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                num_rows=5,
            )
        test_utils.run_dag(self.dag)

        # Read table from db
        df = hook.get_pandas_df(
            f'SELECT * FROM "{os.getenv("SNOWFLAKE_DATABASE")}"."{os.getenv("SNOWFLAKE_SCHEMA")}"."{self.snowflake_table_raw_sql}"'
        )
        assert len(df) == 5
