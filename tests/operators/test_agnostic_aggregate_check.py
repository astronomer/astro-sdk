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
import os
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
CWD = pathlib.Path(__file__).parent

DATABASES_LIST = ["postgres", "bigquery", "sqlite", "snowflake"]


@pytest.fixture(scope="session")
def tables(request):
    aggregate_table = Table(
        "aggregate_check_test",
        database="pagila",
        conn_id="postgres_conn",
        schema="airflow_test_dag",
    )
    aggregate_table_bigquery = Table(
        "aggregate_check_test",
        conn_id="bigquery",
        schema=DEFAULT_SCHEMA,
    )
    aggregate_table_sqlite = Table("aggregate_check_test", conn_id="sqlite_conn")
    aggregate_table_snowflake = Table(
        table_name=get_table_name("aggregate_check_test"),
        database=os.getenv("SNOWFLAKE_DATABASE"),  # type: ignore
        schema=os.getenv("SNOWFLAKE_SCHEMA"),  # type: ignore
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),  # type: ignore
        conn_id="snowflake_conn",
    )
    path = str(CWD) + "/../data/homes_merge_1.csv"
    tables = {
        "postgres": aggregate_table,
        "bigquery": aggregate_table_bigquery,
        "sqlite": aggregate_table_sqlite,
        "snowflake": aggregate_table_snowflake,
    }
    for _, table in tables.items():
        aql.load_file(
            path=path,
            output_table=table,
        ).operator.execute({"run_id": "foo"})

    yield tables


@pytest.mark.parametrize("database", DATABASES_LIST)
def test_exact_value(sample_dag, database, tables):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(tables[database])
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            greater_than=4,
            less_than=4,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("database", DATABASES_LIST)
def test_range_values(sample_dag, database, tables):
    aggregate_table = Table(
        "aggregate_check_test",
        database="pagila",
        conn_id="postgres_conn",
        schema="airflow_test_dag",
    )

    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        table = get_table(tables[database])
        aql.aggregate_check(
            table=table,
            check="select count(*) FROM {{table}}",
            greater_than=2,
            less_than=6,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("database", DATABASES_LIST)
def test_out_of_range_value(sample_dag, database, tables):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            aggregate_table = get_table(tables[database])
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=10,
                less_than=20,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("database", DATABASES_LIST)
def test_equal_to_param(sample_dag, database, tables):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(tables[database])
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            equal_to=4,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("database", DATABASES_LIST)
def test_only_less_than_param(sample_dag, database, tables):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            aggregate_table = get_table(tables[database])
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                less_than=3,
            )
        run_dag(sample_dag)


@pytest.mark.parametrize("database", DATABASES_LIST)
def test_only_greater_than_param(sample_dag, database, tables):
    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with sample_dag:
        aggregate_table = get_table(tables[database])
        aql.aggregate_check(
            table=aggregate_table,
            check="select count(*) FROM {{table}}",
            greater_than=3,
        )
    run_dag(sample_dag)


@pytest.mark.parametrize("database", DATABASES_LIST)
def test_all_three_params_provided_priority_given_to_equal_to_param(
    sample_dag, database, tables
):
    """param:greater_than should be less than or equal to param:less_than"""

    @aql.transform
    def get_table(input_table: Table):
        return "SELECT * FROM {{input_table}}"

    with pytest.raises(ValueError):
        with sample_dag:
            aggregate_table = get_table(tables[database])
            aql.aggregate_check(
                table=aggregate_table,
                check="select count(*) FROM {{table}}",
                greater_than=20,
                less_than=10,
                equal_to=4,
            )
        run_dag(sample_dag)
