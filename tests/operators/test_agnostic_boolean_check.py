"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import os
import pathlib
import unittest.mock

import utils as test_utils
from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.sql.operators.agnostic_boolean_check import (
    AgnosticBooleanCheck,
    Check,
    boolean_check,
)
from astro.sql.table import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestBooleanCheckOperator(unittest.TestCase):
    """
    Test Boolean Check Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.postgres_table = "boolean_check_test"
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=Table(
                cls.postgres_table,
                conn_id="postgres_conn",
                database="pagila",
                schema="public",
            ),
        ).operator.execute({"run_id": "foo"})

        cls.snowflake_table = test_utils.get_table_name("boolean_check_test")

        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=Table(
                conn_id="snowflake_conn",
                table_name=cls.snowflake_table,
                schema=os.getenv("SNOWFLAKE_SCHEMA"),
                database=os.getenv("SNOWFLAKE_DATABASE"),
                warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            ),
        ).operator.execute({"run_id": "foo"})

    @classmethod
    def tearDownClass(cls):
        test_utils.drop_table_snowflake(
            table_name=cls.snowflake_table,
            schema=os.getenv("SNOWFLAKE_SCHEMA"),
            database=os.getenv("SNOWFLAKE_DATABASE"),
            warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
            conn_id="snowflake_conn",
        )

    def clear_run(self):
        self.run = False

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

    def test_happyflow_postgres_success(self):
        try:
            a = boolean_check(
                table=Table(
                    self.postgres_table,
                    database="pagila",
                    conn_id="postgres_conn",
                ),
                checks=[Check("test_1", f"{self.postgres_table}.rooms > 3")],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_happyflow_postgres_fail(self):
        try:
            a = boolean_check(
                table=Table(
                    self.postgres_table,
                    database="pagila",
                    conn_id="postgres_conn",
                ),
                checks=[
                    Check("test_1", f"{self.postgres_table}.rooms > 7"),
                    Check("test_2", f"{self.postgres_table}.beds >= 3"),
                ],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError:
            assert True

    def test_happyflow_snowflake_success(self):
        try:

            a = boolean_check(
                table=Table(
                    conn_id="snowflake_conn",
                    table_name=self.snowflake_table,
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                checks=[Check("test_1", " rooms > 3")],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_happyflow_snowflake_fail(self):
        try:
            a = boolean_check(
                table=Table(
                    conn_id="snowflake_conn",
                    table_name=self.snowflake_table,
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
                checks=[
                    Check("test_1", " rooms > 7"),
                    Check("test_2", " beds >= 3"),
                ],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError:
            assert True
