"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:
    python3 -m unittest tests.operators.test_postgres_decorator.TestPostgresDecorator.test_postgres

"""

import logging
import unittest.mock

import pandas as pd
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
import astronomer_sql_decorator.sql as aql
from astronomer_sql_decorator.sql.types import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


def drop_table(table_name, postgres_conn):
    cursor = postgres_conn.cursor()
    cursor.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE;")
    postgres_conn.commit()
    cursor.close()
    postgres_conn.close()


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

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)
        return f

    def test_postgres(self):
        @aql.transform(conn_id="postgres_conn", database="pagila")
        def sample_pg(input_table: Table):
            return "SELECT * FROM {input_table} WHERE last_name LIKE 'G%%'"

        self.create_and_run_task(sample_pg, (), {"input_table": "actor"})

    def test_postgres_join(self):
        self.hook_target = PostgresHook(
            postgres_conn_id="postgres_conn", schema="pagila"
        )

        drop_table(table_name="my_table", postgres_conn=self.hook_target.get_conn())

        @aql.transform(conn_id="postgres_conn", database="pagila")
        def sample_pg(
            actor: Table, film_actor_join: Table, output_table_name, unsafe_parameter
        ):
            return (
                "SELECT {actor}.actor_id, first_name, last_name, COUNT(film_id) "
                "FROM {actor} JOIN {film_actor_join} ON {actor}.actor_id = {film_actor_join}.actor_id "
                "WHERE last_name LIKE {unsafe_parameter} GROUP BY {actor}.actor_id"
            )

        self.create_and_run_task(
            sample_pg,
            (),
            {
                "actor": "actor",
                "film_actor_join": "film_actor",
                "unsafe_parameter": "G%%",
                "output_table_name": "my_table",
            },
        )
        # Read table from db
        df = pd.read_sql(f"SELECT * FROM my_table", con=self.hook_target.get_conn())
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

        @aql.run_raw_sql(conn_id="postgres_conn", database="pagila")
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
                "actor": "actor",
                "film_actor_join": "film_actor",
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
