from __future__ import annotations

import random
import shutil
import string
from pathlib import Path

import pytest
from airflow.models import DAG, Connection, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session

from astro.table import MAX_TABLE_NAME_LENGTH
from sql_cli.dag_generator import Workflow
from sql_cli.project import Project
from sql_cli.workflow_directory_parser import SqlFile, WorkflowFile

CWD = Path(__file__).parent


DEFAULT_DATE = timezone.datetime(2016, 1, 1)

UNIQUE_HASH_SIZE = 16


@pytest.fixture()
def root_directory():
    return CWD / "tests" / "workflows" / "basic"


@pytest.fixture()
def root_directory_cycle():
    return CWD / "tests" / "workflows" / "cycle"


@pytest.fixture()
def root_directory_symlink():
    return CWD / "tests" / "workflows" / "symlink"


@pytest.fixture()
def root_directory_multiple_operators():
    return CWD / "tests" / "workflows" / "multiple_operators"


@pytest.fixture()
def root_directory_unsupported_operator():
    return CWD / "tests" / "workflows" / "unsupported_operator"


@pytest.fixture()
def root_directory_dags():
    return CWD / "tests" / "test_dag"


@pytest.fixture()
def dags_directory():
    return CWD / "tests" / ".airflow" / "dags"


@pytest.fixture()
def sql_file(root_directory, dags_directory):
    return SqlFile(
        root_directory=root_directory,
        path=root_directory / "a.sql",
        target_directory=dags_directory,
    )


@pytest.fixture()
def sql_file_with_parameters(root_directory, dags_directory):
    return SqlFile(
        root_directory=root_directory,
        path=root_directory / "c.sql",
        target_directory=dags_directory,
    )


@pytest.fixture()
def sql_file_in_sub_directory(root_directory, dags_directory):
    return SqlFile(
        root_directory=root_directory,
        path=root_directory / "sub_dir" / "a.sql",
        target_directory=dags_directory,
    )


@pytest.fixture()
def sql_file_with_cycle(root_directory_cycle, dags_directory):
    return SqlFile(
        root_directory=root_directory_cycle,
        path=root_directory_cycle / "d.sql",
        target_directory=dags_directory,
    )


@pytest.fixture()
def workflow_file(root_directory, dags_directory):
    return WorkflowFile(
        root_directory=root_directory,
        path=root_directory / "a.sql",
        target_directory=dags_directory,
    )


@pytest.fixture()
def workflow_file_with_parameters(root_directory, dags_directory):
    return WorkflowFile(
        root_directory=root_directory,
        path=root_directory / "c.sql",
        target_directory=dags_directory,
    )


@pytest.fixture()
def workflow(sql_file):
    return Workflow(
        dag_id="workflow",
        start_date=DEFAULT_DATE,
        workflow_files=[sql_file],
    )


@pytest.fixture()
def workflow_with_parameters(sql_file_with_parameters):
    return Workflow(
        dag_id="workflow_with_parameters",
        start_date=DEFAULT_DATE,
        workflow_files=[sql_file_with_parameters],
    )


@pytest.fixture()
def workflow_with_cycle(sql_file_with_cycle):
    return Workflow(
        dag_id="workflow_with_cycle",
        start_date=DEFAULT_DATE,
        workflow_files=[sql_file_with_cycle],
    )


@pytest.fixture
def sample_dag():
    dag_id = create_unique_table_name(UNIQUE_HASH_SIZE)
    yield DAG(dag_id, start_date=DEFAULT_DATE)
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()


def create_unique_table_name(length: int = MAX_TABLE_NAME_LENGTH) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.

    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id


@pytest.fixture()
def initialised_project(tmp_path):
    proj = Project(tmp_path)
    proj.initialise()
    return proj


@pytest.fixture()
def initialised_project_with_custom_airflow_config(tmp_path):
    airflow_home_dir = tmp_path / "airflow_home"
    dags_dir = airflow_home_dir / "dags"
    dags_dir.mkdir(parents=True, exist_ok=True)
    proj = Project(tmp_path, airflow_home=airflow_home_dir, airflow_dags_folder=dags_dir)
    proj.initialise()
    return proj


@pytest.fixture()
def initialised_project_with_test_config(initialised_project: Project):
    shutil.copytree(
        src=CWD / "tests" / "config" / "test",
        dst=initialised_project.directory / "config" / "test",
    )
    return initialised_project


@pytest.fixture()
def initialised_project_with_sqlite_non_existent_host_path_config(initialised_project: Project):
    sqlite_non_existent_host_path = "sqlite_non_existent_host_path"
    shutil.copytree(
        src=CWD / "tests" / "config" / sqlite_non_existent_host_path,
        dst=initialised_project.directory / "config" / sqlite_non_existent_host_path,
    )
    return initialised_project


@pytest.fixture()
def connections():
    return [
        Connection(conn_id="sqlite_conn", conn_type="sqlite", host="data/imdb.db"),
    ]


@pytest.fixture()
def initialised_project_with_tests_workflows(initialised_project: Project):
    shutil.copytree(
        src=CWD / "tests" / "workflows",
        dst=initialised_project.directory / "workflows",
        dirs_exist_ok=True,
    )
    return initialised_project
