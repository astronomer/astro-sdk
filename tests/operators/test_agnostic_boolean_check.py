"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import os
import pathlib
import unittest.mock

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


def drop_snowflake_table(table_name):
    snowflake_conn = SnowflakeHook(
        snowflake_conn_id="snowflake_conn",
        schema=os.environ["SNOWFLAKE_SCHEMA"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
    )
    snowflake_conn = snowflake_conn.get_conn()
    cursor = snowflake_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    snowflake_conn.commit()
    cursor.close()
    snowflake_conn.close()


class TestBooleanCheckOperator(unittest.TestCase):
    """
    Test Boolean Check Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

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
        aql.load_file(
            path=str(self.cwd) + "/../data/homes_append.csv",
            output_table=Table(
                "boolean_check_test",
                conn_id="postgres_conn",
                database="pagila",
                schema="public",
            ),
        ).operator.execute({"run_id": "foo"})

        drop_snowflake_table("BOOLEAN_CHECK_TEST")
        aql.load_file(
            path=str(self.cwd) + "/../data/homes_append.csv",
            output_table=Table(
                conn_id="snowflake_conn",
                table_name="BOOLEAN_CHECK_TEST",
            ),
        ).operator.execute({"run_id": "foo"})

    def test_happyflow_postgres_success(self):
        try:
            a = boolean_check(
                table=Table(
                    "boolean_check_test",
                    database="pagila",
                    conn_id="postgres_conn",
                ),
                checks=[Check("test_1", "boolean_check_test.rooms > 3")],
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
                    "boolean_check_test",
                    database="pagila",
                    conn_id="postgres_conn",
                ),
                checks=[
                    Check("test_1", "boolean_check_test.rooms > 7"),
                    Check("test_2", "boolean_check_test.beds >= 3"),
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
                table=Table("boolean_check_test", conn_id="snowflake_conn"),
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
                table=Table("boolean_check_test", conn_id="snowflake_conn"),
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
