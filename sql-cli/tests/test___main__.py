import json
import logging
import pathlib
from unittest import mock

import pytest
from airflow.utils.state import State
from conftest import DEFAULT_DATE
from typer.testing import CliRunner

from sql_cli import __version__
from sql_cli.__main__ import app
from sql_cli.configuration import Config
from sql_cli.connections import CONNECTION_ID_OUTPUT_STRING_WIDTH
from tests.utils import list_dir

runner = CliRunner()

CWD = pathlib.Path(__file__).parent


@pytest.mark.parametrize(
    "command,options",
    [
        (
            "generate",
            {
                "--env": "default",
                "--generate-tasks": "generate-tasks",
            },
        ),
        (
            "run",
            {
                "--env": "default",
                "--generate-tasks": "generate-tasks",
            },
        ),
        (
            "validate",
            {
                "--env": "default",
                "--connection": "None",
            },
        ),
    ],
    ids=[
        "generate",
        "run",
        "validate",
    ],
)
def test_defaults(command, options):
    result = runner.invoke(app, [command, "--help"])
    assert result.exit_code == 0
    for name, value in options.items():
        # We expect option name and option value to appear on the same line.
        assert any(name in line and f"[default: {value}]" in line for line in result.stdout.splitlines())


@pytest.mark.parametrize(
    "args",
    [
        ["--help"],
        ["version", "--help"],
    ],
    ids=[
        "group",
        "command",
    ],
)
@pytest.mark.parametrize(
    "env,usage",
    [
        ({}, "Usage: flow"),
        ({"ASTRO_CLI": "Yes"}, "Usage: astro flow"),
    ],
    ids=[
        "sql-cli",
        "astro-cli",
    ],
)
def test_usage(env, usage, args):
    result = runner.invoke(app, args, env=env)
    assert result.exit_code == 0
    assert usage in result.stdout


@pytest.mark.parametrize(
    "env,try_message",
    [
        ({}, "Try 'flow"),
        ({"ASTRO_CLI": "Yes"}, "Try 'astro flow"),
    ],
    ids=[
        "sql-cli",
        "astro-cli",
    ],
)
def test_invalid_option(env, try_message):
    result = runner.invoke(app, ["--foo"], env=env)
    assert result.exit_code == 2
    assert try_message in result.stdout


def test_about():
    result = runner.invoke(app, ["about"])
    assert result.exit_code == 0
    assert "Find out more: https://docs.astronomer.io/astro/cli/sql-cli" in result.stdout


def test_version():
    result = runner.invoke(app, ["version"])
    assert result.exit_code == 0
    assert f"Astro SQL CLI {__version__}" in result.stdout


def test_version_json_output():
    result = runner.invoke(app, ["version", "--json"])
    assert result.exit_code == 0
    result_json = json.loads(result.stdout)
    assert result_json["version"] == __version__


@pytest.mark.parametrize(
    "key,value",
    [
        ("airflow_home", "airflow_home"),
        ("airflow_dags_folder", "airflow_home/dags"),
    ],
)
def test_config_get(key, value, initialised_project_with_custom_airflow_config):
    result = runner.invoke(
        app,
        [
            "config",
            "get",
            "--project-dir",
            initialised_project_with_custom_airflow_config.directory.as_posix(),
            key,
        ],
    )
    assert result.exit_code == 0, result.output
    assert value in result.stdout


def test_config_set_deploy(initialised_project):
    result = runner.invoke(
        app,
        [
            "config",
            "set",
            "deploy",
            "--project-dir",
            initialised_project.directory.as_posix(),
            "--astro-deployment-id",
            "some-deployment",
            "--astro-workspace-id",
            "some-workspace",
        ],
    )
    assert result.exit_code == 0, result.output
    project_config = Config(environment="default", project_dir=initialised_project.directory).to_dict()
    assert project_config["default"]["deployment"]["astro_workspace_id"] == "some-workspace"
    assert project_config["default"]["deployment"]["astro_deployment_id"] == "some-deployment"


@pytest.mark.parametrize(
    "workflow_name,environment",
    [
        ("example_basic_transform", "default"),
        ("example_load_file", "default"),
        ("example_templating", "dev"),
    ],
)
@pytest.mark.parametrize("generate_tasks", ["--generate-tasks", "--no-generate-tasks"])
def test_generate(workflow_name, environment, initialised_project, generate_tasks, logger):
    result = runner.invoke(
        app,
        [
            "generate",
            workflow_name,
            "--env",
            environment,
            "--project-dir",
            initialised_project.directory.as_posix(),
            generate_tasks,
        ],
    )
    assert result.exit_code == 0, result.output
    assert (
        f"The DAG file {initialised_project.airflow_dags_folder}/{workflow_name}.py has been successfully generated. 🎉"
        in result.stdout
    )
    assert logger.manager.disable == logging.CRITICAL


@pytest.mark.parametrize(
    "workflow_name,environment",
    [
        ("example_basic_transform", "default"),
        ("example_load_file", "default"),
        ("example_templating", "dev"),
    ],
)
@pytest.mark.parametrize("generate_tasks", ["--generate-tasks", "--no-generate-tasks"])
def test_generate_custom_airflow_config(
    workflow_name, environment, initialised_project_with_custom_airflow_config, generate_tasks
):
    result = runner.invoke(
        app,
        [
            "generate",
            workflow_name,
            "--env",
            environment,
            "--project-dir",
            initialised_project_with_custom_airflow_config.directory.as_posix(),
            generate_tasks,
        ],
    )
    assert result.exit_code == 0, result.output
    assert (
        f"The DAG file {initialised_project_with_custom_airflow_config.airflow_dags_folder}/{workflow_name}.py "
        f"has been successfully generated. 🎉" in result.stdout
    )


@pytest.mark.parametrize(
    "workflow_name,message",
    [
        ("non_existing", "The workflow non_existing does not exist!"),
        ("cycle", "The workflow cycle contains a cycle! A cycle between d and d has been detected!"),
        ("empty", "The workflow empty does not have any SQL files!"),
    ],
    ids=[
        "non_existing",
        "cycle",
        "empty",
    ],
)
@pytest.mark.parametrize("generate_tasks", ["--generate-tasks", "--no-generate-tasks"])
def test_generate_invalid(workflow_name, message, initialised_project_with_tests_workflows, generate_tasks):
    result = runner.invoke(
        app,
        [
            "generate",
            workflow_name,
            "--project-dir",
            initialised_project_with_tests_workflows.directory.as_posix(),
            generate_tasks,
        ],
    )
    assert result.exit_code == 1
    assert message in result.stdout


def test_generate_with_output_dir(initialised_project, tmp_path):
    result = runner.invoke(
        app,
        [
            "generate",
            "example_basic_transform",
            "--project-dir",
            initialised_project.directory.as_posix(),
            "--output-dir",
            tmp_path.as_posix(),
        ],
    )
    expected_dag_file_path = tmp_path / "example_basic_transform.py"
    assert expected_dag_file_path.exists()
    message = "has been successfully generated."
    assert message in result.stdout
    assert result.exit_code == 0


@pytest.mark.parametrize(
    "workflow_name,exit_code",
    [
        ("example_basic_transform", 0),
        ("non_existing", 1),
        ("cycle", 1),
        ("empty", 1),
    ],
    ids=[
        "example_basic_transform",
        "non_existing",
        "cycle",
        "empty",
    ],
)
def test_generate_verbose(workflow_name, exit_code, initialised_project_with_tests_workflows, logger):
    result = runner.invoke(
        app,
        [
            "generate",
            workflow_name,
            "--project-dir",
            initialised_project_with_tests_workflows.directory.as_posix(),
            "--verbose",
        ],
    )
    assert result.exit_code == exit_code
    if result.exit_code != 0:
        assert result.exception
    assert logger.level == logging.DEBUG


@pytest.mark.parametrize(
    "env,connection,status",
    [
        ("default", "sqlite_conn", "PASSED"),
        ("dev", "sqlite_conn", "PASSED"),
    ],
)
def test_validate(env, connection, status, initialised_project_with_invalid_config, logger):
    result = runner.invoke(
        app,
        [
            "validate",
            initialised_project_with_invalid_config.directory.as_posix(),
            "--env",
            env,
            "--connection",
            connection,
        ],
    )
    assert result.exit_code == 0, result.exception
    assert f"Validating connection(s) for environment '{env}'" in result.stdout
    assert f"Validating connection {connection:{CONNECTION_ID_OUTPUT_STRING_WIDTH}} {status}" in result.stdout
    assert logger.manager.disable == logging.CRITICAL


@pytest.mark.parametrize(
    "env",
    [
        "default",
        "test",
    ],
)
def test_validate_all(env, initialised_project_with_test_config):
    result = runner.invoke(
        app,
        [
            "validate",
            initialised_project_with_test_config.directory.as_posix(),
            "--env",
            env,
        ],
    )
    assert "Validating connection(s)" in result.stdout
    assert result.exit_code == 0, result.stdout


@pytest.mark.parametrize(
    "env,connection,message",
    [
        ("invalid", "sqlite_non_existent_host_path", "Error: Sqlite db does not exist!"),
        ("invalid", "unknown_hook_type", 'Error: Unknown hook type "sqlite2"'),
    ],
    ids=[
        "sqlite_non_existent_host_path",
        "unknown_hook_type",
    ],
)
def test_validate_invalid(env, connection, message, initialised_project_with_invalid_config, logger):
    result = runner.invoke(
        app,
        [
            "validate",
            initialised_project_with_invalid_config.directory.as_posix(),
            "--env",
            env,
            "--connection",
            connection,
        ],
    )
    assert result.exit_code == 0, result.exception
    assert f"Validating connection(s) for environment '{env}'" in result.stdout
    assert message in result.stdout


@pytest.mark.parametrize(
    "env,connection,status,message",
    [
        ("default", "sqlite_conn", "PASSED", "Connection successfully tested"),
        ("dev", "sqlite_conn", "PASSED", "Connection successfully tested"),
    ],
)
def test_validate_verbose(env, connection, status, message, initialised_project, logger):
    result = runner.invoke(
        app,
        [
            "validate",
            initialised_project.directory.as_posix(),
            "--env",
            env,
            "--connection",
            connection,
            "--verbose",
        ],
    )
    assert result.exit_code == 0
    assert f"Validating connection(s) for environment '{env}'" in result.stdout
    assert f"Validating connection {connection:{CONNECTION_ID_OUTPUT_STRING_WIDTH}} {status}" in result.stdout
    assert f"Connection Message: {message}" in result.stdout
    assert logger.level == logging.DEBUG


@pytest.mark.parametrize(
    "workflow_name,environment",
    [
        ("example_basic_transform", "default"),
        ("example_load_file", "default"),
        ("example_templating", "dev"),
    ],
)
@pytest.mark.parametrize("generate_tasks", ["--generate-tasks", "--no-generate-tasks"])
def test_run(workflow_name, environment, initialised_project, generate_tasks, logger):
    result = runner.invoke(
        app,
        [
            "run",
            workflow_name,
            "--env",
            environment,
            "--project-dir",
            initialised_project.directory.as_posix(),
            generate_tasks,
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"Completed running the workflow {workflow_name}." in result.stdout
    assert logger.manager.disable == logging.CRITICAL


@pytest.mark.parametrize(
    "dag_run_state,final_state",
    [
        (State.SUCCESS, "SUCCESS 🚀"),
        (State.FAILED, "FAILED 💥"),
    ],
    ids=[
        "success",
        "failed",
    ],
)
@pytest.mark.parametrize("generate_tasks", ["--generate-tasks", "--no-generate-tasks"])
@mock.patch("sql_cli.dag_runner.run_dag")
def test_run_state(mock_run_dag, initialised_project, generate_tasks, dag_run_state, final_state):
    workflow_name = "example_basic_transform"
    environment = "dev"
    mock_run_dag.return_value = mock.MagicMock(
        state=dag_run_state,
        dag_id=workflow_name,
        start_date=DEFAULT_DATE,
        end_date=DEFAULT_DATE,
    )

    result = runner.invoke(
        app,
        [
            "run",
            workflow_name,
            "--env",
            environment,
            "--project-dir",
            initialised_project.directory.as_posix(),
            generate_tasks,
        ],
    )
    assert result.exit_code == 0, result.output
    assert f"Final state: {final_state}" in result.stdout


@pytest.mark.parametrize(
    "workflow_name,message",
    [
        ("non_existing", "The workflow non_existing does not exist!"),
        ("cycle", "The workflow cycle contains a cycle! A cycle between d and d has been detected!"),
        ("empty", "The workflow empty does not have any SQL files!"),
        ("undefined_variable", "'foo' is undefined"),
        ("missing_table_or_conn_id", "You need to provide a table or a connection id"),
        ("example_templating", "no such table: orders using connection sqlite_conn"),
    ],
    ids=[
        "non_existing",
        "cycle",
        "empty",
        "undefined_variable",
        "missing_table_or_conn_id",
        "example_templating",
    ],
)
@pytest.mark.parametrize("generate_tasks", ["--generate-tasks", "--no-generate-tasks"])
def test_run_invalid(workflow_name, message, initialised_project_with_tests_workflows, generate_tasks):
    result = runner.invoke(
        app,
        [
            "run",
            workflow_name,
            "--project-dir",
            initialised_project_with_tests_workflows.directory.as_posix(),
            generate_tasks,
        ],
    )
    assert result.exit_code == 1
    assert message in result.stdout


@pytest.mark.parametrize(
    "workflow_name,exit_code",
    [
        ("example_basic_transform", 0),
        ("non_existing", 1),
        ("cycle", 1),
        ("empty", 1),
        ("undefined_variable", 1),
        ("missing_table_or_conn_id", 1),
        ("example_templating", 1),
    ],
    ids=[
        "example_basic_transform",
        "non_existing",
        "cycle",
        "empty",
        "undefined_variable",
        "missing_table_or_conn_id",
        "example_templating",
    ],
)
def test_run_verbose(workflow_name, exit_code, initialised_project_with_tests_workflows, logger):
    result = runner.invoke(
        app,
        [
            "run",
            workflow_name,
            "--project-dir",
            initialised_project_with_tests_workflows.directory.as_posix(),
            "--verbose",
        ],
    )
    assert result.exit_code == exit_code
    if result.exit_code != 0:
        assert result.exception
    assert logger.level == logging.DEBUG


def test_init_with_directory(tmp_path):
    result = runner.invoke(app, ["init", tmp_path.as_posix()])
    assert result.exit_code == 0
    assert f"Initialized an Astro SQL project at {tmp_path.as_posix()}" in result.stdout
    assert list_dir(tmp_path)


def test_init_without_directory():
    # Creates a temporary directory and cd into it.
    # This isolates tests that affect the contents of the CWD to prevent them from interfering with each other.
    with runner.isolated_filesystem() as temp_dir:
        temp_path = pathlib.Path(temp_dir)
        assert not list_dir(temp_path)
        result = runner.invoke(app, ["init"])
        assert result.exit_code == 0
        # We are not checking the full temp_dir because in MacOS the temp directory starts with /private
        assert "Initialized an Astro SQL project at" in result.stdout
        assert temp_dir in result.stdout
        assert list_dir(temp_path)


@pytest.mark.parametrize(
    "debug_option,check_output",
    [
        ("--debug", lambda output: "Airflow DB Initialization exited with 0" in output),
        ("--no-debug", lambda output: "Airflow DB Initialization exited with 0" not in output),
    ],
    ids=[
        "debug",
        "no-debug",
    ],
)
def test_init_output(tmp_path, debug_option, check_output):
    result = runner.invoke(app, [debug_option, "init", tmp_path.as_posix()])
    assert result.exit_code == 0
    assert f"Initialized an Astro SQL project at {tmp_path.as_posix()}" in result.stdout
    assert list_dir(tmp_path)
    assert check_output(result.stdout)


@pytest.mark.parametrize(
    "option,files_created",
    [
        ("--airflow-home", True),  # Airflow home should have been initialised with the airflow files
        ("--airflow-dags-folder", False),  # Does not create example DAGs
        ("--data-dir", True),  # Data directory should have been initialised with the examples
    ],
    ids=[
        "airflow-home",
        "airflow-dags-folder",
        "data-dir",
    ],
)
def test_init_option(tmp_path, tmp_path_factory, option, files_created, request):
    option_path = tmp_path_factory.mktemp(request.node.callspec.id)
    result = runner.invoke(
        app,
        [
            "init",
            tmp_path.as_posix(),
            option,
            option_path.as_posix(),
        ],
    )
    assert result.exit_code == 0
    assert f"Initialized an Astro SQL project at {tmp_path.as_posix()}" in result.stdout
    assert list_dir(tmp_path)
    assert bool(list_dir(option_path)) == files_created


@pytest.mark.parametrize(
    "args,log_level",
    [
        (["--debug", "version"], logging.DEBUG),
    ],
    ids=[
        "debug",
    ],
)
def test_main_debug(args, log_level, ext_loggers):
    result = runner.invoke(app, args)
    assert result.exit_code == 0
    assert all(logger.level == log_level for logger in ext_loggers)


@pytest.mark.parametrize(
    "args",
    [
        ["version"],
        ["--no-debug", "version"],
    ],
    ids=[
        "default",
        "no-debug",
    ],
)
def test_main_no_debug(args, ext_loggers):
    result = runner.invoke(app, args)
    assert result.exit_code == 0
    assert all(logger.manager.disable == logging.CRITICAL for logger in ext_loggers)


def test_deploy():
    result = runner.invoke(app, ["deploy"])
    assert result.exit_code == 1
    assert (
        "Please use the Astro CLI to deploy. See https://docs.astronomer.io/astro/cli/sql-cli for details."
        in result.stdout
    )
