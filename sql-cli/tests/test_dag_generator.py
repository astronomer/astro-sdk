import pytest

from sql_cli.dag_generator import DagCycle, generate_dag


def test_sql_files_dag(sql_files_dag, sql_files_dag_with_parameters, sql_file, sql_file_with_parameters):
    """Test that a simple build will return the sql files."""
    assert sql_files_dag.sorted_sql_files() == [sql_file]
    assert sql_files_dag_with_parameters.sorted_sql_files() == [sql_file_with_parameters]


def test_sql_files_dag_raises_exception(sql_files_dag_with_cycle):
    """Test that an exception is being raised when it is not a DAG."""
    with pytest.raises(DagCycle):
        assert sql_files_dag_with_cycle.sorted_sql_files()


def test_generate_dag(root_directory, dags_directory):
    """Test that the whole DAG generation process including sql files parsing works."""
    dag_file = generate_dag(directory=root_directory, dags_directory=dags_directory)
    assert dag_file


def test_with_temp_dirs(root_directory):
    import os
    import shutil
    import tempfile
    from pathlib import Path

    with tempfile.TemporaryDirectory() as tmp_dir:
        shutil.copytree(root_directory, tmp_dir + "/root")
        os.mkdir(Path(tmp_dir) / "target")
        os.mkdir(Path(tmp_dir) / "dag")
        print(tmp_dir)
        generate_dag(
            Path(tmp_dir) / "root",
            target_directory=Path(tmp_dir) / "target",
            dags_directory=Path(tmp_dir) / "dag",
        )
        print(tmp_dir)
