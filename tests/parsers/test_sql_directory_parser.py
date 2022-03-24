import logging
import pathlib
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
from tests.operators import utils as test_utils

log = logging.getLogger(__name__)
DEFAULT_DATE = timezone.datetime(2016, 1, 1)
import os

from airflow.models.param import Param

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
            params={"foo": Param("first_name")},
            template_searchpath=dir_path,
        )

    def tearDown(self):
        super().tearDown()
        with create_session() as session:
            session.query(DagRun).delete()
            session.query(TI).delete()

    def test_parse(self):
        with self.dag:
            rendered_tasks = aql.render("passing_dag")

        assert (
            rendered_tasks.get("agg_orders")
            and rendered_tasks.get("agg_orders").operator.parameters == {}
        )
        assert rendered_tasks.get("join_customers_and_orders")
        join_params = rendered_tasks.get(
            "join_customers_and_orders"
        ).operator.parameters
        assert len(join_params) == 2
        assert join_params["customers_table"].operator.task_id == "customers_table"

    def test_parse_missing_table(self):
        with pytest.raises(AirflowException):
            with self.dag:
                rendered_tasks = aql.render("missing_table_dag")
            test_utils.run_dag(self.dag)

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
                "missing_table_dag",
                agg_orders=agg_orders,
                customers_table=Table("customers_table"),
            )

    def test_parse_frontmatter(self):
        with self.dag:
            rendered_tasks = aql.render("front_matter_dag")
        customers_table_task = rendered_tasks.get("customers_table")
        assert customers_table_task
        assert customers_table_task.operator.database == "foo"
        assert customers_table_task.operator.schema == "bar"

        customer_output_table = customers_table_task.operator.output_table
        assert customer_output_table.table_name == "my_table"
        assert customer_output_table.schema == "my_schema"

        new_customers_table = rendered_tasks.get("get_new_customers")
        new_customer_output_table = new_customers_table.operator.output_table
        assert new_customer_output_table.table_name == ""
        assert new_customer_output_table.schema == None
        assert new_customer_output_table.database == "my_db"
        assert new_customer_output_table.conn_id == "my_conn_id"

        assert (
            new_customers_table.operator.sql
            == "SELECT * FROM {{customers_table}} WHERE member_since > DATEADD(day, -7, '{{ execution_date }}')"
        )

    def test_parse_creates_xcom(self):
        """
        Runs two tasks with a direct dependency, the DAG will fail if task two can not inherit the table produced by task 1
        :return:
        """
        with self.dag:
            cwd = pathlib.Path(__file__).parent

            input_table = aql.load_file(
                path=str(cwd) + "/../data/homes.csv",
                output_table=Table(
                    test_utils.get_table_name("snowflake_render_test"),
                    conn_id="snowflake_conn",
                    schema=os.getenv("SNOWFLAKE_SCHEMA"),
                    database=os.getenv("SNOWFLAKE_DATABASE"),
                    warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
                ),
            )
            rendered_tasks = aql.render("single_task_dag", input_table=input_table)
        test_utils.run_dag(self.dag)

    def test_parse_to_dataframe(self):
        """
        Runs two tasks with a direct dependency, the DAG will fail if task two can not inherit the table produced by task 1
        :return:
        """
        import pandas as pd

        from astro.dataframe import dataframe as adf

        @adf
        def dataframe_func(df: pd.DataFrame):
            print(df.to_string)

        with self.dag:
            rendered_tasks = aql.render("postgres_simple_tasks")
            dataframe_func(rendered_tasks["test_inheritance"])

        test_utils.run_dag(self.dag)


def run_render_dag_with_dataframe(template_searchpath, filename, modelname, dag):
    """
    Runs two tasks with a direct dependency, the DAG will fail if task two can not inherit the table produced by task 1
    :return:
    """
    import pandas as pd

    from astro.dataframe import dataframe as adf

    dag.params = {"foo": Param("first_name")}
    dag.template_searchpath = template_searchpath

    @adf
    def dataframe_func(df: pd.DataFrame):
        print(df.to_string)

    with dag:
        rendered_tasks = aql.render(filename)
        dataframe_func(rendered_tasks[modelname])

    test_utils.run_dag(dag)


def test_render_with_no_template_path(sample_dag):
    run_render_dag_with_dataframe(
        [], dir_path + "/postgres_simple_tasks", "test_inheritance", sample_dag
    )


def test_render_with_one_template_path(sample_dag):
    run_render_dag_with_dataframe(
        [dir_path + "/template_search"],
        "test_searchpath",
        "test_inheritance",
        sample_dag,
    )


def test_render_with_second_template_path(sample_dag):
    run_render_dag_with_dataframe(
        ["foobar", dir_path + "/template_search"],
        "test_searchpath",
        "test_inheritance",
        sample_dag,
    )


def test_render_with_multi_template_path(sample_dag):
    run_render_dag_with_dataframe(
        [dir_path + "/template_search_1", dir_path + "/template_search_2"],
        "test_searchpath",
        "test_inheritance",
        sample_dag,
    )
