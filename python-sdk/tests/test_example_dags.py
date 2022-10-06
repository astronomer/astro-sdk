from __future__ import annotations

from pathlib import Path
from typing import Iterator

import airflow
import pytest
from airflow.models import DAG
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


DAG_BAG = DagBag(Path(__file__).parent.parent / "example_dags", include_examples=False)
AIRFLOW_VERSION_INDICATOR = "airflow_version:"
MINIMUM_AIRFLOW_VERSION = "2.2.5"


def get_airflow_version(dag: DAG) -> str:
    for tag in dag.tags:
        if tag.startswith(AIRFLOW_VERSION_INDICATOR):
            return tag[len(AIRFLOW_VERSION_INDICATOR) :]
    return MINIMUM_AIRFLOW_VERSION


def get_airflow_dags() -> Iterator[tuple[str, str]]:
    for dag_id, dag in DAG_BAG.dags.items():
        yield dag_id, dag, get_airflow_version(dag)


@pytest.mark.parametrize(
    "dag",
    [
        pytest.param(
            dag,
            id=dag_id,
            marks=pytest.mark.skipif(
                airflow.__version__ < dag_airflow_version,
                reason=f"Require Airflow version >= {dag_airflow_version}",
            ),
        )
        for dag_id, dag, dag_airflow_version in get_airflow_dags()
    ],
)
def test_example_dag(session, dag: DAG):
    wrapper_run_dag(dag)


def test_example_dags_loaded():
    assert DAG_BAG.dags


def test_example_dags_no_import_errors():
    assert not DAG_BAG.import_errors
