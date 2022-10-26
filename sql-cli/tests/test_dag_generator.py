from pathlib import Path

import pytest
from conftest import DEFAULT_DATE

from sql_cli.dag_generator import SqlFilesDAG, generate_dag, render_dag
from sql_cli.exceptions import DagCycle, EmptyDag, SqlFilesDirectoryNotFound
from sql_cli.run_dag import run_dag


def test_sql_files_dag(sql_files_dag, sql_files_dag_with_parameters, sql_file, sql_file_with_parameters):
    """Test that a simple build will return the sql files."""
    assert sql_files_dag.sorted_sql_files() == [sql_file]
    assert sql_files_dag_with_parameters.sorted_sql_files() == [sql_file_with_parameters]


def test_sql_files_dag_with_cycle(sql_files_dag_with_cycle):
    """Test that an exception is being raised when it is not a DAG."""
    with pytest.raises(DagCycle):
        assert sql_files_dag_with_cycle.sorted_sql_files()


def test_sql_files_dag_without_sql_files():
    """Test that an exception is being raised when it does not have sql files."""
    with pytest.raises(EmptyDag):
        SqlFilesDAG(dag_id="sql_files_dag_without_sql_files", start_date=DEFAULT_DATE, sql_files=[])


def test_generate_dag(root_directory, dags_directory):
    """Test that the whole DAG generation process including sql files parsing works."""
    dag_file = generate_dag(directory=root_directory, dags_directory=dags_directory)
    assert dag_file


def test_generate_dag_render(test_connections, root_directory, sample_dag):
    dag = render_dag(directory=root_directory, workflow_name=sample_dag.dag_id)
    run_dag(dag)


def test_generate_dag_invalid_directory(root_directory, dags_directory):
    """Test that an exception is being raised when the directory does not exist."""
    with pytest.raises(SqlFilesDirectoryNotFound):
        generate_dag(directory=Path("foo"), dags_directory=dags_directory)
