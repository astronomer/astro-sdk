"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import logging
import pathlib
import unittest

from airflow.models import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.sql.table import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


class TestPostgresTruncateOperator(unittest.TestCase):
    """
    Test Postgres Merge Operator.
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
        self.file_table = aql.load_file(
            path=str(self.cwd) + "/../data/homes_merge_1.csv",
            output_table=Table(
                "truncate_test",
                database="pagila",
                conn_id="postgres_conn",
                schema="public",
            ),
        ).operator.execute({"run_id": "foo"})
        self.file_sqlite_table = aql.load_file(
            path=str(self.cwd) + "/../data/homes_merge_1.csv",
            output_table=Table(
                "truncate_test",
                conn_id="sqlite_conn",
            ),
        ).operator.execute({"run_id": "foo"})

    def test_truncate(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(sql="SELECT * FROM public.truncate_test")
        assert df.count()[0] == 4
        a = aql.truncate(
            table=self.file_table,
        )
        a.execute({"run_id": "foo"})
        df = hook.get_pandas_df(sql="SELECT * FROM public.truncate_test")
        assert df.count()[0] == 0

    def test_sqlite_truncate(self):
        hook = SqliteHook(
            sqlite_conn_id="sqlite_conn",
        )
        df = hook.get_pandas_df(sql="SELECT * FROM truncate_test")
        assert df.count()[0] == 4
        a = aql.truncate(
            table=self.file_sqlite_table,
        )
        a.execute({"run_id": "foo"})
        df = hook.get_pandas_df(sql="SELECT * FROM truncate_test")
        assert df.count()[0] == 0
