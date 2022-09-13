import pytest
from sql_cli.dag_generator import DagCycle, SqlFilesDAG


def test_sql_files_dag_build(sql_file):
    """Test that a simple build will return the sql files."""
    assert SqlFilesDAG(sql_files={sql_file}).build() == [sql_file]


def test_sql_files_dag_build_raise_exception(sql_file_with_cycle):
    """Test that an exception is being raised when it is not a DAG."""
    with pytest.raises(DagCycle):
        assert SqlFilesDAG(sql_files={sql_file_with_cycle}).build()
