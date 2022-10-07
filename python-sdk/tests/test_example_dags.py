from __future__ import annotations

from pathlib import Path

import airflow
import pytest
from airflow.models.dagbag import DagBag
from airflow.utils import timezone
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from packaging import version
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


def get_dag_folder() -> Path:
    """
    Recursively find the dag folder for the current airflow version.

    :return: the dag folder containing the airflow dags.
    """
    base_folder = Path(__file__).parent.parent / "example_dags"
    try:
        return next(
            folder
            for folder in base_folder.rglob("./")
            if version.parse(airflow.__version__) >= version.parse(folder.name)
        )
    except StopIteration:
        raise ValueError(
            "Could not find a dag folder with the same name as the current airflow version!"
        )


DAG_BAG = DagBag(dag_folder=get_dag_folder(), include_examples=False)


@pytest.mark.parametrize("dag_id", DAG_BAG.dag_ids)
def test_example_dag(session, dag_id):
    dag = DAG_BAG.get_dag(dag_id)
    wrapper_run_dag(dag)


def test_example_dags_loaded():
    assert DAG_BAG.dags


def test_example_dags_no_import_errors():
    assert not DAG_BAG.import_errors
