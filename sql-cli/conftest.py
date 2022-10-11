import tempfile
from datetime import datetime
from pathlib import Path

import pytest

from sql_cli.dag_generator import SqlFilesDAG
from sql_cli.sql_directory_parser import SqlFile


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


@pytest.fixture()
def empty_cwd(request, monkeypatch):
    temp_dir = tempfile.TemporaryDirectory()
    monkeypatch.chdir(temp_dir.name)
    yield temp_dir.name
    temp_dir.cleanup()
