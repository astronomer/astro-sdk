from typer.testing import CliRunner

from sql_cli import __version__
from sql_cli.__main__ import app
import pathlib

runner = CliRunner()

CWD = pathlib.Path(__file__).parent

def get_stdout(result) -> str:
    """
    Get the results stdout without line breaks.

    :params result: The result object.

    :returns: the stdout without line breaks.
    """
    return result.stdout.replace("\n", "")


def test_about():
    result = runner.invoke(app, ["about"])
    assert result.exit_code == 0
    assert "Find out more: https://github.com/astronomer/astro-sdk/sql-cli" == get_stdout(result)


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert f"Astro SQL CLI {__version__}" == get_stdout(result)


def test_generate(root_directory, dags_directory):
    result = runner.invoke(
        app,
        [
            "generate",
            root_directory.as_posix(),
            dags_directory.as_posix(),
        ],
    )
    assert result.exit_code == 0
    result_stdout = get_stdout(result)
    assert result_stdout.startswith("The DAG file ")
    assert result_stdout.endswith(" has been successfully generated. 🎉")


def test_validate():
    result = runner.invoke(app, ["validate"])
    assert result.exit_code == 0


def test_run_dag(root_directory_dags):
    result = runner.invoke(
        app,
        [
            "run",
            "example_dataframe",
            root_directory_dags.as_posix(),
        ],
    )
    assert result.exit_code == 0
    result_stdout = get_stdout(result)
    assert "The worst month was 2020-05" in result_stdout

def test_run_sql_project(root_directory, target_directory, dags_directory):
    args = [
            "run",
            "generate",
            root_directory.as_posix(),
            f"{CWD}/test_conn.yaml"
        ]
    print(" ".join(args))
    result = runner.invoke(
        app,
        [
            "run",
            "root",
            root_directory.as_posix(),
            f"{CWD}/test_conn.yaml"
        ],
    )
    assert result.exit_code == 0
    result_stdout = get_stdout(result)
    assert "The worst month was 2020-05" in result_stdout