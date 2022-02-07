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
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.db import check
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
import astro.sql as aql
from astro.sql.table import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)


class TestAggregateCheckOperator(unittest.TestCase):
    """
    Test Postgres Merge Operator.
    """

    cwd = pathlib.Path(__file__).parent

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.aggregate_table = Table(
            "aggregate_check_test",
            database="pagila",
            conn_id="postgres_conn",
            schema="airflow_test_dag",
        )
        cls.aggregate_table_bigquery = Table(
            "aggregate_check_test",
            conn_id="bigquery",
            schema="tmp_astro",
        )
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_merge_1.csv",
            output_table=cls.aggregate_table,
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_merge_1.csv",
            output_table=cls.aggregate_table_bigquery,
        ).operator.execute({"run_id": "foo"})

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

    def test_exact_value(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(
            sql="SELECT * FROM airflow_test_dag.aggregate_check_test"
        )
        assert df.count()[0] == 4
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                greater_than=4,
                less_than=4,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_exact_value_biquery(self):
        hook = BigQueryHook(
            bigquery_conn_id="bigquery", use_legacy_sql=False, gcp_conn_id="bigquery"
        )
        df = hook.get_pandas_df(sql="SELECT * FROM tmp_astro.aggregate_check_test")
        assert df.count()[0] == 4
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table_bigquery,
                check="select count(*) FROM tmp_astro.aggregate_check_test",
                greater_than=4,
                less_than=4,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_range_values(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(
            sql="SELECT * FROM airflow_test_dag.aggregate_check_test"
        )
        assert df.count()[0] == 4
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                greater_than=2,
                less_than=6,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_out_of_range_value(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(
            sql="SELECT * FROM airflow_test_dag.aggregate_check_test"
        )
        assert df.count()[0] == 4
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                greater_than=10,
                less_than=20,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError:
            assert True

    def test_invalid_values(self):
        """param:greater_than should be less than or equal to param:less_than"""

        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                greater_than=20,
                less_than=10,
            )
            assert False
        except ValueError:
            assert True

    def test_postgres_exact_number_of_rows(self):
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                greater_than=4,
                less_than=4,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_invalid_params_no_test_values(self):
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
            )
            assert False
        except ValueError:
            assert True

    def test_equal_to_param(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(
            sql="SELECT * FROM airflow_test_dag.aggregate_check_test"
        )
        assert df.count()[0] == 4
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                equal_to=4,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_only_less_than_param(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(
            sql="SELECT * FROM airflow_test_dag.aggregate_check_test"
        )
        assert df.count()[0] == 4
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                less_than=3,
            )
            result = a.execute({"run_id": "foo"})
            assert False
        except ValueError:
            assert True

    def test_only_greater_than_param(self):
        hook = PostgresHook(schema="pagila", postgres_conn_id="postgres_conn")
        df = hook.get_pandas_df(
            sql="SELECT * FROM airflow_test_dag.aggregate_check_test"
        )
        assert df.count()[0] == 4
        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                greater_than=3,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError:
            assert False

    def test_all_three_params_provided_priority_given_to_equal_to_param(self):
        """param:greater_than should be less than or equal to param:less_than"""

        try:
            a = aql.aggregate_check(
                table=self.aggregate_table,
                check="select count(*) FROM airflow_test_dag.aggregate_check_test",
                greater_than=20,
                less_than=10,
                equal_to=4,
            )
            assert False
        except ValueError:
            assert True
