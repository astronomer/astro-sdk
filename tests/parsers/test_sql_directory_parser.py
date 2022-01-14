import logging
import pathlib
import unittest.mock
from unittest import mock

import pandas as pd
import pytest
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils import timezone
from airflow.utils.session import create_session
from airflow.utils.state import State
from airflow.utils.types import DagRunType

# Import Operator
import astro.sql as aql
from astro import dataframe as df
from astro.sql.table import Table, TempTable

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
import os
import time

dir_path = os.path.dirname(os.path.realpath(__file__))


class TestSQLParsing(unittest.TestCase):
    def setUp(self):
        super().setUp()
        self.dag = DAG(
            "test_dag",
            default_args={
                "owner": "airflow",
                "start_date": DEFAULT_DATE,
            },
        )

    def tearDown(self):
        super().tearDown()
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_parse(self):
        with self.dag:
            rendered_tasks = aql.render_directory(dir_path + "/passing_dag")

        assert (
            rendered_tasks.get("agg_orders")
            and rendered_tasks.get("agg_orders").operator.parameters == {}
        )
        assert rendered_tasks.get("join_customers_and_orders")
        join_params = rendered_tasks.get(
            "join_customers_and_orders"
        ).operator.parameters
        assert len(join_params) == 2
        assert (
            join_params["customers_table"].operator.task_id
            == "render_directory.customers_table"
        )

    def test_parse_missing_table(self):
        with pytest.raises(AirflowException):
            with self.dag:
                rendered_tasks = aql.render_directory(dir_path + "/missing_table_dag")

    def test_parse_missing_table_with_inputs(self):
        with self.dag:
            rendered_tasks = aql.render_directory(
                dir_path + "/missing_table_dag",
                agg_orders=Table("foo"),
                customers_table=Table("customers_table"),
            )

    def test_parse_missing_table_with_input_and_upstream(self):
        with self.dag:
            agg_orders = aql.load_file("s3://foo")
            rendered_tasks = aql.render_directory(
                dir_path + "/missing_table_dag",
                agg_orders=agg_orders,
                customers_table=Table("customers_table"),
            )
