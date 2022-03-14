import logging
import pathlib
import unittest.mock
from unittest import mock

import pandas as pd
import pytest
from airflow.executors.debug_executor import DebugExecutor
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

import astro.sql as aql
from astro import dataframe as adf
from astro.settings import SCHEMA
from astro.sql.table import Table, TempTable
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
import time


@pytest.fixture
def dag():
    return DAG(
        "test_dag",
        default_args={
            "owner": "airflow",
            "start_date": DEFAULT_DATE,
        },
    )


@pytest.fixture(autouse=True)
def cleanup():
    yield
    with create_session() as session:
        session.query(DagRun).delete()
        session.query(TI).delete()


@pytest.fixture
def output_table(request):
    table_type = request.param
    if table_type == "None":
        return TempTable()
    elif table_type == "partial":
        return Table("my_table")
    elif table_type == "full":
        return Table("my_table", database="pagila", conn_id="postgres_conn")


@pytest.mark.parametrize("output_table", ["None", "partial", "full"], indirect=True)
def test_postgres_to_dataframe_partial_output(output_table, dag):
    hook_target = PostgresHook(postgres_conn_id="postgres_conn", schema="pagila")

    @aql.transform
    def sample_pg(input_table: Table):
        return "SELECT * FROM {{input_table}} WHERE last_name LIKE 'G%%'"

    @adf
    def validate(df: pd.DataFrame):
        assert len(df) == 12
        assert df.iloc[0].to_dict()["first_name"] == "PENELOPE"

    with dag:
        pg_output = sample_pg(
            input_table=Table(
                table_name="actor", conn_id="postgres_conn", database="pagila"
            ),
            output_table=output_table,
        )
        df_count = validate(df=pg_output)
    test_utils.run_dag(dag)
