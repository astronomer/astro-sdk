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
import pathlib
import random
import unittest.mock

import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.hooks.sqlite_hook import SqliteHook
from airflow.models import DAG, DagRun
from airflow.providers.google.cloud.hooks.bigquery import BigQueryHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone

import astro.sql as aql
from astro.settings import SCHEMA
from astro.sql.table import Table

from tests.operators.utils import run_dag

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
            schema=SCHEMA,
        )
        cls.aggregate_table_sqlite = Table(
            "aggregate_check_test", conn_id="sqlite_conn"
        )
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_merge_1.csv",
            output_table=cls.aggregate_table,
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_merge_1.csv",
            output_table=cls.aggregate_table_bigquery,
        ).operator.execute({"run_id": "foo"})
        aql.load_file(
            path=str(cls.cwd) + "/../data/homes_merge_1.csv",
            output_table=cls.aggregate_table_sqlite,
        ).operator.execute({"run_id": "foo"})

    def clear_run(self):
        self.run = False

    def get_dag(self):
        very_high_number = 99999999
        return DAG(
            "test_dag_" + str(random.randint(0, very_high_number)),
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )

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
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            aggregate_table = get_table(self.aggregate_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=4,
                less_than=4,
            )
        run_dag(dag)

    def test_exact_value_biquery(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            aggregate_table_bigquery = get_table(self.aggregate_table_bigquery)
            aql.aggregate_check(
                table=aggregate_table_bigquery,
                check="select count(*) FROM {{table}}",
                greater_than=4,
                less_than=4,
            )
        run_dag(dag)

    def test_exact_value_sqlite(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            aggregate_table_sqlite = get_table(self.aggregate_table_sqlite)
            aql.aggregate_check(
                table=aggregate_table_sqlite,
                check="select count(*) FROM {{table}}",
                greater_than=4,
                less_than=4,
            )
        run_dag(dag)

    def test_range_values(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            aggregate_table = get_table(self.aggregate_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=2,
                less_than=6,
            )
        run_dag(dag)

    def test_out_of_range_value(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(BackfillUnfinished):
            dag = get_dag()
            with dag:
                aggregate_table = get_table(self.aggregate_table)
                aql.aggregate_check(
                    table=aggregate_table,
                    check="select count(*) FROM {{table}}",
                    greater_than=10,
                    less_than=20,
                )
            run_dag(dag)

    def test_invalid_values(self):
        """param:greater_than should be less than or equal to param:less_than"""

        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(ValueError):
            dag = get_dag()
            with dag:
                aggregate_table = get_table(self.aggregate_table)
                aql.aggregate_check(
                    table=aggregate_table,
                    check="select count(*) FROM {{table}}",
                    greater_than=20,
                    less_than=10,
                )
            run_dag(dag)

    def test_invalid_params_no_test_values(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(ValueError):
            dag = get_dag()
            with dag:
                aggregate_table = get_table(self.aggregate_table)
                aql.aggregate_check(
                    table=aggregate_table, check="select count(*) FROM {{table}}"
                )
            run_dag(dag)

    def test_equal_to_param(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            aggregate_table = get_table(self.aggregate_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                equal_to=4,
            )
        run_dag(dag)

    def test_only_less_than_param(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(BackfillUnfinished):
            dag = get_dag()
            with dag:
                aggregate_table = get_table(self.aggregate_table)
                aql.aggregate_check(
                    table=aggregate_table,
                    check="select count(*) FROM {{table}}",
                    less_than=3,
                )
            run_dag(dag)

    def test_only_greater_than_param(self):
        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        dag = get_dag()
        with dag:
            aggregate_table = get_table(self.aggregate_table)
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=3,
            )
        run_dag(dag)

    def test_all_three_params_provided_priority_given_to_equal_to_param(self):
        """param:greater_than should be less than or equal to param:less_than"""

        @aql.transform
        def get_table(input_table: Table):
            return "SELECT * FROM {{input_table}}"

        with pytest.raises(ValueError):
            dag = get_dag()
            with dag:
                aggregate_table = get_table(self.aggregate_table)
                aql.aggregate_check(
                    table=aggregate_table,
                    check="select count(*) FROM {{table}}",
                    greater_than=20,
                    less_than=10,
                    equal_to=4,
                )
            run_dag(dag)
