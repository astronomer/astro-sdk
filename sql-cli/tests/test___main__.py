import pathlib
import tempfile

from typer.testing import CliRunner

from sql_cli import __version__
from sql_cli.__main__ import app
from tests.utils import list_dir

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


def test_run(root_directory, dags_directory):
    result = runner.invoke(
        app,
        [
            "run",
            root_directory.as_posix(),
            dags_directory.as_posix(),
            "--connection-file",
            (CWD / "test_conn.yaml").as_posix(),
        ],
    )
    assert result.exit_code == 0
    result_stdout = get_stdout(result)
    assert "Dagrun sql_files final state: success" in result_stdout


def test_init_with_directory():
    with tempfile.TemporaryDirectory() as dir_name:
        result = runner.invoke(app, ["init", dir_name])
        assert result.exit_code == 0
        expected_msg = f"Initialized an Astro SQL project at {dir_name}"
        assert expected_msg in get_stdout(result)
        assert list_dir(dir_name)


def test_init_with_custom_airflow_config():
    with tempfile.TemporaryDirectory() as dir_name:
        result = runner.invoke(
            app, ["init", dir_name, "--airflow-home", "/some/home", "--airflow-dags-folder", "/tmp"]
        )
        assert result.exit_code == 0
        expected_msg = f"Initialized an Astro SQL project at {dir_name}"
        assert expected_msg in get_stdout(result)
        assert list_dir(dir_name)


def test_init_without_directory(empty_cwd):
    # Creates a temporary directory and cd into it.
    # This isolates tests that affect the contents of the CWD to prevent them from interfering with each other.
    with runner.isolated_filesystem() as temp_dir:
        assert not list_dir(temp_dir)
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        expected_msg = "Initialized an Astro SQL project at"
        result_stdout = get_stdout(result)
        # We are not checking the full temp_dir because in MacOS the temp directory starts with /private
        assert result_stdout.startswith(expected_msg)
        assert result_stdout.endswith(temp_dir)
        assert list_dir(temp_dir)
