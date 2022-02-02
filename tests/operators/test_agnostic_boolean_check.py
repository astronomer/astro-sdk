"""
Unittest module to test Operators.
Requires the unittest, pytest, and requests-mock Python libraries.
"""

import logging
import os
import pathlib
import time
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

# from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def get_table_name(prefix):
    """get unique table name"""
    return prefix + "_" + str(int(time.time()))


def drop_table_snowflake(
    table_name: str,
    conn_id: str = "snowflake_conn",
    schema: str = os.environ["SNOWFLAKE_SCHEMA"],
    database: str = os.environ["SNOWFLAKE_DATABASE"],
    warehouse: str = os.environ["SNOWFLAKE_WAREHOUSE"],
):
    hook = SnowflakeHook(
        snowflake_conn_id=conn_id,
        schema=schema,
        database=database,
        warehouse=warehouse,
    )
    snowflake_conn = hook.get_conn()
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
        cls.table = "boolean_check_test"
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=Table(
                cls.table,
                conn_id="postgres_sqla_conn",
                database="pagila",
                schema="public",
            ),
        ).operator.execute({"run_id": "foo"})

        cls.snowflake_table = get_table_name("boolean_check_test")
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

        cls.table = "boolean_check_test"
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_append.csv",
            output_table=Table(
                cls.table,
                conn_id="bigquery",
                schema="tmp_astro",
            ),
        ).operator.execute({"run_id": "foo"})

    @classmethod
    def tearDownClass(cls):
        drop_table_snowflake(
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
                    self.table,
                    database="pagila",
                    conn_id="postgres_sqla_conn",
                ),
                checks=[Check("test_1", f"{self.table}.rooms > 3")],
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
                    self.table,
                    database="pagila",
                    conn_id="postgres_sqla_conn",
                ),
                checks=[
                    Check("test_1", f"{self.table}.rooms > 7"),
                    Check("test_2", f"{self.table}.beds >= 3"),
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

    def test_happyflow_bigquery_success(self):
        try:
            a = boolean_check(
                table=Table(
                    self.table,
                    conn_id="bigquery",
                    schema="tmp_astro",
                ),
                checks=[Check("test_1", f"{self.table}.rooms > 3")],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_happyflow_bigquery_fail(self):
        try:
            a = boolean_check(
                table=Table(
                    self.table,
                    conn_id="bigquery",
                    schema="tmp_astro",
                ),
                checks=[
                    Check("test_1", f"{self.table}.rooms > 7"),
                    Check("test_2", f"{self.table}.beds >= 3"),
                ],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError:
            assert True
