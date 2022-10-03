import pytest
from typer.testing import CliRunner

from sql_cli.main import generate_dag
from sql_cli.main import app


@pytest.mark.freeze_time("2022-09-28")
def test_generate_dag(root_directory, target_directory, dags_directory):
    """Test that the whole DAG generation process including sql files parsing works."""
    generate_dag(
        directory=root_directory,
        target_directory=target_directory,
        dags_directory=dags_directory,
    )


runner = CliRunner()


def test_main_version():
    result = runner.invoke(app, ["about"])
    assert result.exit_code == 0
    assert "Find out more" in result.stdout


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert "Astro SQL CLI 0.0.1" in result.stdout
