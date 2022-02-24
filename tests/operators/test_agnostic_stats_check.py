"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import copy
import logging
import os
import pathlib
import unittest.mock
from typing import Dict

import pytest
from airflow.models import DAG
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.sql.table import Table
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent
TABLES_CACHE: Dict[str, Dict] = {}


def create_tables_objects(tables):
    results = []

    copy_table = copy.deepcopy(tables)
    for _, table in copy_table.items():
        path = table.pop("path")
        name = table.pop("name")

        if name in TABLES_CACHE:
            results.append(TABLES_CACHE[name])
            continue

        astro_table_object = Table(name, **table)
        table["name"] = name
        aql.load_file(
            path=str(CWD) + path,
            output_table=astro_table_object,
        ).operator.execute({"run_id": "foo"})

        TABLES_CACHE[name] = astro_table_object
        results.append(astro_table_object)

    return results


tables = [
    {
        "table_1": {
            "path": "/../data/homes.csv",
            "name": "stats_check_test_1",
            "conn_id": "postgres_conn",
            "database": "pagila",
            "schema": "public",
        },
        "table_2": {
            "path": "/../data/homes2.csv",
            "name": "stats_check_test_2",
            "conn_id": "postgres_conn",
            "database": "pagila",
            "schema": "public",
        },
        "table_3": {
            "path": "/../data/homes3.csv",
            "name": "stats_check_test_3",
            "conn_id": "postgres_conn",
            "database": "pagila",
            "schema": "public",
        },
    },
    {
        "table_1": {
            "path": "/../data/homes.csv",
            "name": test_utils.get_table_name("stats_check_test_4"),
            "conn_id": "snowflake_conn",
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        },
        "table_2": {
            "path": "/../data/homes2.csv",
            "name": test_utils.get_table_name("stats_check_test_5"),
            "conn_id": "snowflake_conn",
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        },
        "table_3": {
            "path": "/../data/homes3.csv",
            "name": test_utils.get_table_name("stats_check_test_6"),
            "conn_id": "snowflake_conn",
            "database": os.getenv("SNOWFLAKE_DATABASE"),
            "schema": os.getenv("SNOWFLAKE_SCHEMA"),
            "warehouse": os.getenv("SNOWFLAKE_WAREHOUSE"),
        },
    },
    {
        "table_1": {
            "path": "/../data/homes.csv",
            "name": "stats_check_test_7",
            "conn_id": "bigquery",
            "database": "pagila",
            "schema": "ASTROFLOW_CI",
        },
        "table_2": {
            "path": "/../data/homes2.csv",
            "name": "stats_check_test_8",
            "conn_id": "bigquery",
            "schema": "ASTROFLOW_CI",
        },
        "table_3": {
            "path": "/../data/homes3.csv",
            "name": "stats_check_test_9",
            "conn_id": "bigquery",
            "schema": "ASTROFLOW_CI",
        },
    }
    # {
    #     "table_1" : {
    #         "path" : "/../data/homes.csv",
    #         "name" : 'stats_check_test_1',
    #         "conn_id" : "sqlite_conn",
    #     },
    #     "table_2" : {
    #         "path" : "/../data/homes2.csv",
    #         "name" : 'stats_check_test_1',
    #         "conn_id" : "sqlite_conn",
    #     },
    #     "table_3" : {
    #         "path" : "/../data/homes2.csv",
    #         "name" : 'stats_check_test_1',
    #         "conn_id" : "sqlite_conn",
    #     }
    # },
]


@pytest.mark.parametrize("tables", tables)
class TestStatsCheckOperator:
    """
    Test Stats Check Operator.
    """

    cwd = pathlib.Path(__file__).parent

    def test_stats_check_postgres_outlier_exists(self, tables):
        tables_objects = create_tables_objects(tables)
        try:
            a = aql.stats_check(
                main_table=tables_objects[0],
                compare_table=tables_objects[1],
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert False
        except ValueError as e:
            assert True

    def test_stats_check_postgres_outlier_not_exists(self, tables):
        tables_objects = create_tables_objects(tables)
        try:
            a = aql.stats_check(
                main_table=tables_objects[0],
                compare_table=tables_objects[2],
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
            a.execute({"run_id": "foo"})
            assert True
        except ValueError as e:
            assert False
