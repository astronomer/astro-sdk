from typer.testing import CliRunner

from sql_cli import __version__
from sql_cli.__main__ import app

runner = CliRunner()


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
