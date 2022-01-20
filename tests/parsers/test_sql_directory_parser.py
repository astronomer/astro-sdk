import logging
import unittest.mock

import pytest
from airflow.exceptions import AirflowException
from airflow.models import DAG, DagRun
from airflow.models import TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session

# Import Operator
import astro.sql as aql
from astro.sql.table import Table

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
import os

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
            rendered_tasks = aql.render(dir_path + "/passing_dag")

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
            join_params["customers_table"].operator.task_id == "render.customers_table"
        )

    def test_parse_missing_table(self):
        with pytest.raises(AirflowException):
            with self.dag:
                rendered_tasks = aql.render(dir_path + "/missing_table_dag")

    def test_parse_missing_table_with_inputs(self):
        with self.dag:
            rendered_tasks = aql.render(
                dir_path + "/missing_table_dag",
                agg_orders=Table("foo"),
                customers_table=Table("customers_table"),
            )

    def test_parse_missing_table_with_input_and_upstream(self):
        with self.dag:
            agg_orders = aql.load_file("s3://foo")
            rendered_tasks = aql.render(
                dir_path + "/missing_table_dag",
                agg_orders=agg_orders,
                customers_table=Table("customers_table"),
            )

    def test_parse_frontmatter(self):
        with self.dag:
            rendered_tasks = aql.render(dir_path + "/front_matter_dag")
        customers_table_task = rendered_tasks.get("customers_table")
        assert customers_table_task
        assert customers_table_task.operator.database == "foo"
        assert customers_table_task.operator.schema == "bar"
        new_customers_table = rendered_tasks.get("get_new_customers")
        assert (
            new_customers_table.operator.sql
            == "SELECT * FROM {customers_table} WHERE member_since > DATEADD(day, -7, '{{ execution_date }}')"
        )

    def test_parse_with_load(self):
        with self.dag:
            rendered_tasks = aql.render(dir_path + "/load_table_dag")
