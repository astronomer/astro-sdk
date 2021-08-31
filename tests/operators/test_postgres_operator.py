"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

Run test:

    python3 -m unittest tests.operators.test_postgres_operator.TestSampleOperator.test_load_s3_to_db

"""

import json
import logging
import unittest.mock
from unittest import mock
from pandas import DataFrame
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
import os
import sqlalchemy

from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType
from airflow.models import Connection
from airflow import settings

from airflow.providers.postgres.hooks.postgres import PostgresHook
# Import Operator
from astronomer_sql_decorator.operators.postgres_decorator import postgres_decorator

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


# Mock the `conn_sample` Airflow connection
@mock.patch.dict('os.environ', AIRFLOW_CONN_CONN_SAMPLE='http://https%3A%2F%2Fwww.httpbin.org%2F')
class TestSampleOperator(unittest.TestCase):
    """
    Test Sample Operator.
    """

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        with create_session() as session:
            postgres_connection = Connection(
                conn_id="postgres_conn",
                conn_type="postgres",
                host="localhost",
                port=5432,
                login="postgres",
            )
            session.query(DagRun).delete()
            session.query(TI).delete()
            session.query(Connection).delete()
            session.add(postgres_connection)

    def setUp(self):
        super().setUp()
        self.clear_run()
        self.addCleanup(self.clear_run)
        # Airflow db session
        self.session = settings.Session()

        from astronomer_sql_decorator.example_dags.demo_sql_from_csv_and_s3 import demo_dag
        self.dag = demo_dag

        # Create TIs
        self._tis = [TI(task=task, execution_date=DEFAULT_DATE, state=State.RUNNING)
                     for task in self.dag.tasks]

        # Write TIs to db
        tis = list(map(self.session.merge, self._tis))

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=timezone.utcnow(),
            state=State.RUNNING,
        )

        # Writes to `dag_run` table
        self.session.commit()

    def clear_run(self):
        self.run = False

    def tearDown(self):
        super().tearDown()

        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_dataframe_func(self):
        @postgres_decorator(postgres_conn_id="postgres_conn", to_dataframe=True)
        def print_table(input_df: DataFrame):
            print(input_df.to_string)

        with self.dag:
            x = print_table(input_table="bar")

        dr = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        x.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_postgres(self):

        dag = DAG('test_dag', default_args={
            'owner': 'airflow', 'start_date': DEFAULT_DATE})

        @postgres_decorator(postgres_conn_id="postgres_conn", database="pagila")
        def sample_pg(input_table):
            return "SELECT * FROM %(input_table)s WHERE last_name LIKE 'G%%'"

        with dag:
            f = sample_pg("actor")

        dr = dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        f.operator.run(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE)

    def test_load_s3_to_sql_db(self):

        # Execute task (Writes to xcom)
        ti = self._tis[0]
        ti._run_raw_task()

        # Generate target db hook
        self.hook_target = PostgresHook(
            postgres_conn_id='postgres_conn',
            schema='astro')

        # Using a sqlalchemy engine because `to_sql` with `hook_target.get_conn()` returns error
        engine = sqlalchemy.create_engine(
            os.environ['AIRFLOW_CONN_POSTGRES_CONN']).connect()

        # Read table from db
        output_table_name = ti.task.op_kwargs.get('csv_path')
        df = pd.read_sql(f'SELECT * FROM {output_table_name}', con=engine)

        # Assert output table structure
        assert df.to_json(
        ) == '{"Sell":{"0":142,"1":175,"2":129,"3":138,"4":232,"5":135,"6":150,"7":207}}'

    def test_load_local_csv_to_sql_db(self):

        # Execute task (Writes to xcom)
        ti = self._tis[1]
        ti._run_raw_task()

        # Generate target db hook
        self.hook_target = PostgresHook(
            postgres_conn_id='postgres_conn',
            schema='astro')

        # Using a sqlalchemy engine because `to_sql` with `hook_target.get_conn()` returns error
        engine = sqlalchemy.create_engine(
            os.environ['AIRFLOW_CONN_POSTGRES_CONN']).connect()

        # Read table from db
        output_table_name = ti.task.op_kwargs.get('csv_path')
        df = pd.read_sql(f'SELECT * FROM {output_table_name}', con=engine)

        # Assert output table structure
        assert df.to_json(
        ) == '{"Sell":{"0":142,"1":175,"2":129,"3":138,"4":232,"5":135,"6":150,"7":207}}'


if __name__ == '__main__':
    unittest.main()
