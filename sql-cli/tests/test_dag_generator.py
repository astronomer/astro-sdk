from pathlib import Path

import pytest
from conftest import DEFAULT_DATE

from sql_cli.dag_generator import SqlFilesDAG, render_dag
from sql_cli.exceptions import DagCycle, EmptyDag, SqlFilesDirectoryNotFound
from sql_cli.run_dag import run_dag


def test_sql_files_dag_without_sql_files():
    """Test that an exception is being raised when it does not have sql files."""
    with pytest.raises(EmptyDag):
        SqlFilesDAG(dag_id="sql_files_dag_without_sql_files", start_date=DEFAULT_DATE, sql_files=[])


def test_generate_dag_render(test_connections, root_directory):
    rendered_dag = render_dag(directory=root_directory, dag_id="foo")
    run_dag(rendered_dag)

