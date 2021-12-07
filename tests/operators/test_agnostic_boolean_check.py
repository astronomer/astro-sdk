"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import pathlib
import unittest.mock

from airflow.models import DAG
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
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
            output_conn_id="postgres_conn",
            output_table_name="boolean_check_test",
        ).operator.execute({"run_id": "foo"})

        aql.load_file(
            path=str(self.cwd) + "/../data/homes_append.csv",
            output_conn_id="snowflake_conn",
            output_table_name="BOOLEAN_CHECK_TEST",
        ).operator.execute({"run_id": "foo"})

    def test_happyflow_postgres_success(self):
        try:
            a = aql.boolean_check(
                table=Table(
                    "boolean_check_test",
                    database="pagila",
                    conn_id="postgres_conn",
                ),
                checks=[aql.Check("test_1", " boolean_check_test.rooms > 3")],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_happyflow_postgres_fail(self):
        try:
            a = aql.boolean_check(
                table=Table(
                    "boolean_check_test",
                    database="pagila",
                    conn_id="postgres_conn",
                ),
                checks=[
                    aql.Check("test_1", " boolean_check_test.rooms > 7"),
                    aql.Check("test_2", " boolean_check_test.beds >= 3"),
                ],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError:
            assert True

    def test_happyflow_snowflake_success(self):
        try:
            a = aql.boolean_check(
                table=Table("boolean_check_test", conn_id="snowflake_conn"),
                checks=[aql.Check("test_1", " rooms > 3")],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_happyflow_snowflake_fail(self):
        try:
            a = aql.boolean_check(
                table=Table("boolean_check_test", conn_id="snowflake_conn"),
                checks=[
                    aql.Check("test_1", " rooms > 7"),
                    aql.Check("test_2", " beds >= 3"),
                ],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError:
            assert True
