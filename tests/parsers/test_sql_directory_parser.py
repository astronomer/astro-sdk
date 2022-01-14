import logging
import pathlib
import unittest.mock
from unittest import mock

import pandas as pd
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
            x = aql.parse_directory(dir_path)

        dagrun = self.dag.create_dagrun(
            run_id=DagRunType.MANUAL.value,
            start_date=timezone.utcnow(),
            execution_date=DEFAULT_DATE,
            state=State.RUNNING,
        )
        print(x)
