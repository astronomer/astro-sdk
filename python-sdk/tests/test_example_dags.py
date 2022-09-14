import os

import airflow
import pytest
from airflow.models.dagbag import DagBag
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from tests.sql.operators import utils as test_utils

RETRY_ON_EXCEPTIONS = []
try:
    from google.api_core.exceptions import Forbidden, TooManyRequests
    from pandas_gbq.exceptions import GenericGBQException

    RETRY_ON_EXCEPTIONS.extend([Forbidden, TooManyRequests, GenericGBQException])
except ModuleNotFoundError:
    pass


DEFAULT_DATE = timezone.datetime(2016, 1, 1)


@retry(
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(RETRY_ON_EXCEPTIONS),
    wait=wait_exponential(multiplier=10, min=10, max=60),  # values in seconds
)
def wrapper_run_dag(dag):
    test_utils.run_dag(dag, account_for_cleanup_failure=True)


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
        "example_append",
        "example_load_file",
        "example_transform",
        "example_merge_bigquery",
        "example_transform_file",
        "calculate_popular_movies",
    ],
)
def test_example_dag(session, dag_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db = DagBag(dir_path + "/../example_dags")
    dag = db.get_dag(dag_id)

    if dag is None:
        raise NameError(f"The DAG with dag_id: {dag_id} was not found")
    wrapper_run_dag(dag)


@pytest.mark.skipif(
    airflow.__version__ < "2.3.0", reason="Require Airflow version >= 2.3.0"
)
@pytest.mark.parametrize(
    "dag_id",
    [
        "example_dynamic_map_task",
        "example_dynamic_task_template",
    ],
)
def test_example_dynamic_task_map_dag(session, dag_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db = DagBag(dir_path + "/../example_dags")
    dag = db.get_dag(dag_id)

    if dag is None:
        raise NameError(f"The DAG with dag_id: {dag_id} was not found")
    wrapper_run_dag(dag)


@pytest.mark.skipif(
    airflow.__version__ < "2.4.0", reason="Require Airflow version >= 2.4.0"
)
@pytest.mark.parametrize(
    "dag_id",
    [
        "example_dataset_producer",
    ],
)
def test_example_dataset_dag(session, dag_id):
    dir_path = os.path.dirname(os.path.realpath(__file__))
    db = DagBag(dir_path + "/../example_dags")
    dag = db.get_dag(dag_id)

    if dag is None:
        raise NameError(f"The DAG with dag_id: {dag_id} was not found")
    wrapper_run_dag(dag)
