"""
Unittest module to test Operators.

Requires the unittest, pytest, and requests-mock Python libraries.

"""

import copy
import logging
import os
import pathlib
from typing import Dict

import pytest
from airflow.exceptions import BackfillUnfinished
from airflow.models import DAG
from airflow.utils import timezone

# Import Operator
import astro.sql as aql
from astro.settings import SCHEMA
from astro.sql.table import Table
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
CWD = pathlib.Path(__file__).parent
TABLES_CACHE: Dict[str, Dict] = {}


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        "bigquery",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        [
            {
                "path": str(CWD) + "/../data/homes.csv",
                "load_table": True,
                "is_temp": False,
                "param": {
                    "schema": SCHEMA,
                    "table_name": test_utils.get_table_name("test_stats_check_1"),
                },
            },
            {
                "path": str(CWD) + "/../data/homes3.csv",
                "load_table": True,
                "is_temp": False,
                "param": {
                    "schema": SCHEMA,
                    "table_name": test_utils.get_table_name("test_stats_check_2"),
                },
            },
        ],
    ],
    indirect=True,
)
def test_stats_check_postgres_outlier_exists(sample_dag, sql_server, test_table):
    with sample_dag:
        aql.stats_check(
            main_table=test_table[0],
            compare_table=test_table[1],
            checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
            max_rows_returned=10,
        )
        test_utils.run_dag(sample_dag)


@pytest.mark.parametrize(
    "sql_server",
    [
        "snowflake",
        "postgres",
        "bigquery",
    ],
    indirect=True,
)
@pytest.mark.parametrize(
    "test_table",
    [
        [
            {
                "path": str(CWD) + "/../data/homes.csv",
                "load_table": True,
                "is_temp": False,
                "param": {
                    "schema": SCHEMA,
                    "table_name": test_utils.get_table_name("test_stats_check_1"),
                },
            },
            {
                "path": str(CWD) + "/../data/homes2.csv",
                "load_table": True,
                "is_temp": False,
                "param": {
                    "schema": SCHEMA,
                    "table_name": test_utils.get_table_name("test_stats_check_2"),
                },
            },
        ],
    ],
    indirect=True,
)
def test_stats_check_outlier_exists(sample_dag, sql_server, test_table):
    with pytest.raises(BackfillUnfinished):
        with sample_dag:
            aql.stats_check(
                main_table=test_table[0],
                compare_table=test_table[1],
                checks=[aql.OutlierCheck("room_check", {"rooms": "rooms"}, 2, 0.0)],
                max_rows_returned=10,
            )
        test_utils.run_dag(sample_dag)
