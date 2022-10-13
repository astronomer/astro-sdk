import random
import string
from datetime import datetime
from pathlib import Path

import pytest
from airflow.models import DAG, DagRun, TaskInstance as TI
from airflow.utils import timezone
from airflow.utils.session import create_session

from astro.table import MAX_TABLE_NAME_LENGTH
from sql_cli.dag_generator import SqlFilesDAG
from sql_cli.sql_directory_parser import SqlFile

DEFAULT_DATE = timezone.datetime(2016, 1, 1)

UNIQUE_HASH_SIZE = 16


@pytest.fixture()
def root_directory():
    return Path.cwd() / "tests" / "workflows" / "sql_files"


@pytest.fixture()
def root_directory_cycle():
    return Path.cwd() / "tests" / "workflows" / "sql_files_cycle"


@pytest.fixture()
def root_directory_symlink():
    return Path.cwd() / "tests" / "workflows" / "sql_files_symlink"


@pytest.fixture()
def root_directory_dags():
    return Path.cwd() / "tests" / "test_dag"

@pytest.fixture()
def dags_directory():
    return Path.cwd() / "tests" / ".airflow" / "dags"


@pytest.fixture()
def target_directory():
    return Path.cwd() / "tests" / ".airflow" / "dags" / ".sql"


@pytest.fixture()
def sql_file(root_directory, target_directory):
    return SqlFile(
        root_directory=root_directory,
        path=root_directory / "a.sql",
        target_directory=target_directory,
    )


@pytest.fixture()
def sql_file_with_parameters(root_directory, target_directory):
    return SqlFile(
        root_directory=root_directory,
        path=root_directory / "c.sql",
        target_directory=target_directory,
    )


@pytest.fixture()
def sql_file_in_sub_directory(root_directory, target_directory):
    return SqlFile(
        root_directory=root_directory,
        path=root_directory / "sub_dir" / "a.sql",
        target_directory=target_directory,
    )


@pytest.fixture()
def sql_file_with_cycle(root_directory_cycle, target_directory):
    return SqlFile(
        root_directory=root_directory_cycle,
        path=root_directory_cycle / "d.sql",
        target_directory=target_directory,
    )


@pytest.fixture()
def sql_files_dag(sql_file):
    return SqlFilesDAG(dag_id="sql_files_dag", start_date=datetime(2022, 10, 4), sql_files=[sql_file])


@pytest.fixture()
def sql_files_dag_with_parameters(sql_file_with_parameters):
    return SqlFilesDAG(
        dag_id="sql_files_dag_with_parameters",
        start_date=datetime(2022, 10, 4),
        sql_files=[sql_file_with_parameters],
    )


@pytest.fixture()
def sql_files_dag_with_cycle(sql_file_with_cycle):
    return SqlFilesDAG(
        dag_id="sql_files_dag_with_cycle",
        start_date=datetime(2022, 10, 4),
        sql_files=[sql_file_with_cycle],
    )


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


@pytest.fixture
def sample_dag():
    dag_id = create_unique_table_name(UNIQUE_HASH_SIZE)
    yield DAG(dag_id, start_date=DEFAULT_DATE)
    with create_session() as session_:
        session_.query(DagRun).delete()
        session_.query(TI).delete()
