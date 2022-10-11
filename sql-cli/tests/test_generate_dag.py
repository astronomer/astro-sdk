import pytest

from sql_cli.generate_dag import generate_dag


@pytest.mark.freeze_time("2022-09-28")
def test_generate_dag(root_directory, target_directory, dags_directory):
    """Test that the whole DAG generation process including sql files parsing works."""
    generate_dag(
        directory=root_directory,
        target_directory=target_directory,
        dags_directory=dags_directory,
    )
