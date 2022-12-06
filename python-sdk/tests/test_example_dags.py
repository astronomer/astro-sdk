from __future__ import annotations

from pathlib import Path

import airflow
import pytest
from airflow.models.dagbag import DagBag
from airflow.models.dagrun import DagRun
from airflow.utils.db import create_default_connections
from airflow.utils.session import provide_session
from packaging.version import Version
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from .sql.operators import utils as test_utils

RETRY_ON_EXCEPTIONS = []
try:
    from google.api_core.exceptions import Forbidden, TooManyRequests
    from pandas_gbq.exceptions import GenericGBQException

    RETRY_ON_EXCEPTIONS.extend([Forbidden, TooManyRequests, GenericGBQException])
except ModuleNotFoundError:
    pass


@retry(
    stop=stop_after_attempt(3),
    retry=retry_if_exception_type(tuple(RETRY_ON_EXCEPTIONS)),
    wait=wait_exponential(multiplier=10, min=10, max=60),  # values in seconds
)
def wrapper_run_dag(dag) -> DagRun:
    dag_run = test_utils.run_dag(dag)
    return dag_run


@provide_session
def get_session(session=None):
    create_default_connections(session)
    return session


@pytest.fixture()
def session():
    return get_session()


MIN_VER_DAG_FILE: dict[str, list[str]] = {
    "2.3": ["example_dynamic_task_template.py", "example_bigquery_dynamic_map_task.py"],
    "2.4": ["example_datasets.py"],
}

# Sort descending based on Versions and convert string to an actual version
MIN_VER_DAG_FILE_VER: dict[Version, list[str]] = {
    Version(version): MIN_VER_DAG_FILE[version]
    for version in sorted(MIN_VER_DAG_FILE, key=Version, reverse=True)
}


def get_dag_bag() -> DagBag:
    """Create a DagBag by adding the files that are not supported to .airflowignore"""
    example_dags_dir = Path(__file__).parent.parent / "example_dags"
    airflow_ignore_file = example_dags_dir / ".airflowignore"

    with open(airflow_ignore_file, "w+") as file:
        for min_version, files in MIN_VER_DAG_FILE_VER.items():
            if Version(airflow.__version__) < min_version:
                print(f"Adding {files} to .airflowignore")
                file.writelines([f"{file}\n" for file in files])

    print(".airflowignore contents: ")
    print(airflow_ignore_file.read_text())
    dag_bag = DagBag(example_dags_dir, include_examples=False)
    return dag_bag


PRE_DEFINED_ORDER = [
    "example_dataset_producer",
    "example_dataset_consumer",
]


def order(dag_id: str) -> int:
    if dag_id in PRE_DEFINED_ORDER:
        return PRE_DEFINED_ORDER.index(dag_id)
    return -1


dag_bag = get_dag_bag()


@pytest.mark.parametrize("dag_id", sorted(dag_bag.dag_ids, key=order))
def test_example_dag(session, dag_id: str):
    dag = dag_bag.get_dag(dag_id)
    dag_run = wrapper_run_dag(dag)
    print(dag_run)
    for ti in dag_run.get_task_instances():
        if ti.state == "failed":
            assert False


def test_example_dags_loaded_with_no_errors():
    assert dag_bag.dags
    assert not dag_bag.import_errors
