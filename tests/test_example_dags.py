import os

import pytest
from airflow.models.dagbag import DagBag
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session

from tests.sql.operators import utils as test_utils

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
        "example_snowflake_partial_table_with_append",
        "example_sqlite_load_transform",
        "example_dynamic_map_task",
        "example_append",
        "example_load_file",
        "example_merge_bigquery",
    ],
)
def test_example_dag(session, dag_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db = DagBag(dir_path + "/../example_dags")
    dag = db.get_dag(dag_id)

    if dag is None:
        raise NameError(f"The DAG with dag_id: {dag_id} was not found")
    test_utils.run_dag(dag, account_for_cleanup_failure=True)
