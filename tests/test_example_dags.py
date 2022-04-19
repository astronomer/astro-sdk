import os

import pytest
from airflow.executors.debug_executor import DebugExecutor
from airflow.models.dagbag import DagBag
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from airflow.utils.state import State

DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


@pytest.mark.parametrize(
    "dag_id",
    [
        "example_amazon_s3_postgres",
        "example_amazon_s3_postgres_load_and_save",
        "example_amazon_s3_snowflake_transform",
        "example_google_bigquery_gcs_load_and_save",
        "example_postgres_render",
        "example_snowflake_partial_table_with_append",
        "example_snowflake_render",
        "example_sqlite_load_transform",
    ],
)
def test_example_dag(session, dag_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db = DagBag(dir_path + "/../example_dags")
    dag = db.get_dag(dag_id)

    if dag is None:
        raise NameError(f"The DAG with dag_id: {dag_id} was not found")

    dag.clear(start_date=DEFAULT_DATE, end_date=DEFAULT_DATE, dag_run_state=State.NONE)

    dag.run(
        executor=DebugExecutor(),
        start_date=DEFAULT_DATE,
        end_date=DEFAULT_DATE,
        run_at_least_once=True,
    )
