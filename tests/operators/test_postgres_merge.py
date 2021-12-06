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

"""

import logging
import math
import pathlib
import unittest.mock

from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

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


class TestPostgresMergeOperator(unittest.TestCase):
    """
    Test Postgres Merge Operator.
    """

    cwd = pathlib.Path(__file__).parent

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
        aql.load_file(
            path=str(self.cwd) + "/../data/homes_merge_1.csv",
            output_table=Table(
                table_name="merge_test_1", conn_id="postgres_conn", database="pagila"
            ),
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(self.cwd) + "/../data/homes_merge_2.csv",
            output_table=Table(
                table_name="merge_test_2", conn_id="postgres_conn", database="pagila"
            ),
        ).operator.execute({"run_id": "foo"})

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

    def test_merge_basic_single_key(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        hook.run(sql="ALTER TABLE merge_test_1 ADD CONSTRAINT airflow UNIQUE (list)")
        a = aql.merge(
            target_table="merge_test_1",
            merge_table="merge_test_2",
            merge_keys=["list"],
            target_columns=["list"],
            merge_columns=["list"],
            conn_id="postgres_conn",
            conflict_strategy="ignore",
            database="pagila",
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql="SELECT * FROM merge_test_1")
        assert df.age.to_list()[:-1] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.age.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert math.isnan(df.taxes.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert df.list.to_list() == [160, 180, 132, 140, 240]
        assert df.sell.to_list()[:-1] == [142, 175, 129, 138]
        assert math.isnan(df.taxes.to_list()[-1])

    def test_merge_basic_ignore(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        hook.run(
            sql="ALTER TABLE merge_test_1 ADD CONSTRAINT airflow UNIQUE (list,sell)"
        )
        a = aql.merge(
            target_table="merge_test_1",
            merge_table="merge_test_2",
            merge_keys=["list", "sell"],
            target_columns=["list", "sell"],
            merge_columns=["list", "sell"],
            conn_id="postgres_conn",
            conflict_strategy="ignore",
            database="pagila",
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql="SELECT * FROM merge_test_1")
        assert df.age.to_list()[:-1] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.age.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert math.isnan(df.taxes.to_list()[-1])
        assert df.taxes.to_list()[:-1] == [3167.0, 4033.0, 1471.0, 3204.0]
        assert df.list.to_list() == [160, 180, 132, 140, 240]
        assert df.sell.to_list() == [142, 175, 129, 138, 232]

    def test_merge_basic_update(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        hook.run(
            sql="ALTER TABLE merge_test_1 ADD CONSTRAINT airflow UNIQUE (list,sell)"
        )
        a = aql.merge(
            target_table="merge_test_1",
            merge_table="merge_test_2",
            merge_keys=["list", "sell"],
            target_columns=["list", "sell", "taxes"],
            merge_columns=["list", "sell", "age"],
            conn_id="postgres_conn",
            conflict_strategy="update",
            database="pagila",
        )
        a.execute({"run_id": "foo"})

        df = hook.get_pandas_df(sql="SELECT * FROM merge_test_1")
        assert df.taxes.to_list() == [1, 1, 1, 1, 1]
        assert df.age.to_list()[:-1] == [60.0, 12.0, 41.0, 22.0]
        assert math.isnan(df.age.to_list()[-1])
