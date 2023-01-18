from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING

import typer
from dotenv import load_dotenv

import sql_cli
from sql_cli.astro.command import AstroCommand
from sql_cli.astro.group import AstroGroup
from sql_cli.cli import config as cli_config
from sql_cli.cli.utils import resolve_project_dir
from sql_cli.constants import (
    DEFAULT_BASE_AIRFLOW_HOME,
    DEFAULT_DAGS_FOLDER,
    DEFAULT_DATA_DIR,
    EXT_LOGGER_NAMES,
    LOGGER_NAME,
)
from sql_cli.exceptions import ConnectionFailed, DagCycle, EmptyDag, WorkflowFilesDirectoryNotFound
from sql_cli.settings import STATE
from sql_cli.utils.rich import RichHandler, rprint

if TYPE_CHECKING:
    from sql_cli.project import Project  # pragma: no cover

load_dotenv()
app = typer.Typer(
    name="flow",
    cls=AstroGroup,
    add_completion=False,
    context_settings={"help_option_names": ["-h", "--help"]},
    rich_markup_mode="rich",
)
app.add_typer(cli_config.app, name="config", hidden=True)
log = logging.getLogger(LOGGER_NAME)


def set_verbose_mode(verbose: bool) -> None:
    """
    Sets the logging level for the sql-cli to be more verbose.

    :param verbose: Whether verbose logs should be enabled.
    """
    log.propagate = False
    log.addHandler(RichHandler(markup=True))
    if verbose:
        log.setLevel(logging.DEBUG)
        log.manager.disable = logging.NOTSET
    else:
        log.setLevel(logging.CRITICAL)


def set_debug_mode(debug: bool) -> None:
    """
    Sets the logging level for all external dependencies.

    :param debug: Whether debug logs should be enabled.
    """
    STATE["debug"] = debug

    if debug:
        for logger_name in EXT_LOGGER_NAMES:
            logger = logging.getLogger(logger_name)
            logger.propagate = True
            logger.setLevel(logging.DEBUG)
            logger.manager.disable = logging.NOTSET
    else:
        for logger_name in EXT_LOGGER_NAMES:
            logger = logging.getLogger(logger_name)
            logger.propagate = False
            logger.setLevel(logging.CRITICAL)
        logging.disable()  # skipcq PY-A6006


@app.command(
    cls=AstroCommand,
    help="Print the SQL CLI version.",
)
def version() -> None:
    rprint("Astro SQL CLI", sql_cli.__version__)


@app.command(
    cls=AstroCommand,
    help="Print additional information about the project.",
)
def about() -> None:
    rprint("Find out more: https://docs.astronomer.io/astro/cli/sql-cli")


@app.command(
    cls=AstroCommand,
    help="Generate the Airflow DAG from a directory of SQL files.",
)
def generate(
    workflow_name: str = typer.Argument(
        default=...,
        show_default=False,
        help="name of the workflow directory within workflows directory.",
    ),
    env: str = typer.Option(
        default="default",
        help="environment to run in",
    ),
    project_dir: Path = typer.Option(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    generate_tasks: bool = typer.Option(
        default=True,
        help="whether to explicitly generate the tasks in your SQL CLI DAG",
        show_default=True,
    ),
    verbose: bool = typer.Option(False, help="Whether to show verbose output", show_default=True),
    output_dir: Path = typer.Option(
        default=None, help="to export a SQL workflow to a specific directory", show_default=False
    ),
) -> None:
    from sql_cli.project import Project

    set_verbose_mode(verbose)
    project_dir_absolute = resolve_project_dir(project_dir)
    project = Project(project_dir_absolute)
    project.load_config(env)

    rprint(
        f"\nGenerating the DAG file from workflow [bold blue]{workflow_name}[/bold blue]"
        f" for [bold]{env}[/bold] environment..\n"
    )
    dag_file = _generate_dag(
        project=project, workflow_name=workflow_name, generate_tasks=generate_tasks, output_dir=output_dir
    )
    rprint("The DAG file", dag_file.resolve(), "has been successfully generated. 🎉")


@app.command(
    cls=AstroCommand,
    help="""
    Validate Airflow connection(s) provided in the configuration file for the given environment.
    """,
)
def validate(
    project_dir: Path = typer.Argument(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    env: str = typer.Option(
        default="default",
        help="(Optional) Environment used to declare the connections to be validated",
    ),
    connection: str = typer.Option(
        default=None,
        help="(Optional) Identifier of the connection to be validated. By default checks all the env connections.",
    ),
    verbose: bool = typer.Option(False, help="Whether to show verbose output", show_default=True),
) -> None:
    from sql_cli.connections import validate_connections
    from sql_cli.project import Project

    set_verbose_mode(verbose)
    project_dir_absolute = resolve_project_dir(project_dir)
    project = Project(project_dir_absolute)
    project.load_config(environment=env)

    rprint(f"Validating connection(s) for environment '{env}'")
    validate_connections(connections=project.connections, connection_id=connection)


@app.command(
    cls=AstroCommand,
    help="""
    Run a workflow locally. This task assumes that there is a local airflow DB (can be a SQLite file), that has been
    initialized with Airflow tables.
    """,
)
def run(
    workflow_name: str = typer.Argument(
        default=...,
        show_default=False,
        help="name of the workflow directory within workflows directory.",
    ),
    env: str = typer.Option(
        metavar="environment",
        default="default",
        help="environment to run in",
    ),
    project_dir: Path = typer.Option(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    generate_tasks: bool = typer.Option(
        default=True,
        help="whether to explicitly generate the tasks in your SQL CLI DAG",
        show_default=True,
    ),
    verbose: bool = typer.Option(False, help="Whether to show verbose output", show_default=True),
) -> None:
    from airflow.utils.state import State

    from sql_cli.dag_runner import run_dag
    from sql_cli.project import Project
    from sql_cli.utils.airflow import get_dag

    set_verbose_mode(verbose)
    project_dir_absolute = resolve_project_dir(project_dir)
    project = Project(project_dir_absolute)
    project.load_config(env)

    # TODO: Validate all DAG connections before running

    # Generate DAG before running
    dag_file = _generate_dag(project=project, workflow_name=workflow_name, generate_tasks=generate_tasks)

    dag = get_dag(dag_id=workflow_name, subdir=dag_file.parent.as_posix(), include_examples=False)

    rprint(f"\nRunning the workflow [bold blue]{dag.dag_id}[/bold blue] for [bold]{env}[/bold] environment\n")
    try:
        dr = run_dag(
            dag,
            run_conf=project.get_env_airflow_config(env),
            connections={c.conn_id: c for c in project.connections},
            verbose=verbose,
        )
    except ConnectionFailed as connection_failed:
        log.exception(connection_failed.args[0])
        rprint(
            f"  [bold red]{connection_failed}[/bold red] using connection [bold]{connection_failed.conn_id}[/bold]"
        )
        raise typer.Exit(code=1)
    except Exception as exception:
        log.exception(exception.args[0])
        rprint(f"  [bold red]{exception}[/bold red]")
        raise typer.Exit(code=1)
    final_state = dr.state
    if dr.state == State.SUCCESS:
        final_state = "[bold green]SUCCESS 🚀[/bold green]"
    elif dr.state == State.FAILED:
        final_state = "[bold red]FAILED 💥[/bold red]"
    rprint(f"Completed running the workflow {dr.dag_id}. Final state: {final_state}")
    elapsed_seconds = (dr.end_date - dr.start_date).total_seconds()
    rprint(f"Total elapsed time: [bold blue]{elapsed_seconds:.2f}s[/bold blue]")


@app.command(
    cls=AstroCommand,
    help="""
    Initialise a project structure to write workflows using SQL files.

    Examples of usage:
    $ flow init
    $ flow init .
    $ flow init project_name

    By default, the project structure includes:
    ├── config: withholds configuration, e.g. database connections, within each environment directory
    ├── data: directory which contains datasets, including SQLite databases used by the examples
    └── workflows: directory where SQL workflows are declared, by default has two examples of workflow

    Next steps:
    * Update the file `config/default/configuration.yaml` to declare database connections.
    * Create SQL workflows within the `workflows` folder.
    """,
)
def init(
    project_dir: Path = typer.Argument(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    airflow_home: Path = typer.Option(
        None,
        dir_okay=True,
        help=f"(Optional) Set the Airflow Home. Default: {DEFAULT_BASE_AIRFLOW_HOME}/<env>/",
        show_default=False,
    ),
    airflow_dags_folder: Path = typer.Option(
        None,
        dir_okay=True,
        help=f"(Optional) Set the DAGs Folder. Default: {DEFAULT_DAGS_FOLDER}",
        show_default=False,
    ),
    data_dir: Path = typer.Option(
        None,
        dir_okay=True,
        help=f"(Optional) Set the data directory for local file databases such as sqlite. Default: {DEFAULT_DATA_DIR}",
        show_default=False,
    ),
    verbose: bool = typer.Option(False, help="Whether to show verbose output", show_default=True),
) -> None:
    from sql_cli.project import Project

    set_verbose_mode(verbose)
    project_dir_absolute = resolve_project_dir(project_dir)
    project = Project(project_dir_absolute, airflow_home, airflow_dags_folder, data_dir)
    project.initialise()
    rprint("Initialized an Astro SQL project at", project.directory)


@app.command(
    cls=AstroCommand,
    help="""Deploy workflows to Astro Cloud.""",
)
def deploy(
    workflow_name: str = typer.Option(  # skipcq: PYL-W0613
        default=None,
        show_default=False,
        help="name of the workflow directory within workflows directory.",
    ),
    env: str = typer.Option(  # skipcq: PYL-W0613
        metavar="environment",
        default="default",
        help="environment to deploy",
    ),
    project_dir: Path = typer.Option(  # skipcq: PYL-W0613
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
) -> None:
    rprint(
        "Please use the Astro CLI to deploy. See https://docs.astronomer.io/astro/cli/sql-cli for details."
    )
    raise typer.Exit(code=1)


def _generate_dag(
    project: Project, workflow_name: str, generate_tasks: bool, output_dir: Path | None = None
) -> Path:
    """
    Helper function for generating DAGs with proper exceptions. Moved here since this function is used by
    multiple commands

    :param project: project object containing project metadata
    :param workflow_name: name of the workflow for this DAG
    :param generate_tasks: whether to render each task individually or use the render function
    :param output_dir: directory where the DAG will be exported to (used in conjunction with name).
       Defaults to airflow_dags_folder.
    :return: the path to the generated dag_file
    """
    from sql_cli import dag_generator
    from sql_cli.utils.airflow import check_for_dag_import_errors

    try:
        dag_file = dag_generator.generate_dag(
            directory=project.directory / project.workflows_directory / workflow_name,
            dags_directory=output_dir or project.airflow_dags_folder,
            generate_tasks=generate_tasks,
        )
    except EmptyDag as empty_dag:
        log.exception(empty_dag.args[0])
        rprint(f"[bold red]The workflow {workflow_name} does not have any SQL files![/bold red]")
        raise typer.Exit(code=1)
    except WorkflowFilesDirectoryNotFound as workflow_files_directory_not_found:
        log.exception(workflow_files_directory_not_found.args[0])
        rprint(f"[bold red]The workflow {workflow_name} does not exist![/bold red]")
        raise typer.Exit(code=1)
    except DagCycle as dag_cycle:
        log.exception(dag_cycle.args[0])
        rprint(f"[bold red]The workflow {workflow_name} contains a cycle! {dag_cycle}[/bold red]")
        raise typer.Exit(code=1)
    import_errors = check_for_dag_import_errors(dag_file)
    if import_errors:
        log.error(import_errors)
        all_errors = "\n\n".join(list(import_errors.values()))
        rprint(f"[bold red]Workflow failed to render[/bold red]\n errors found:\n\n {all_errors}")
        raise typer.Exit(code=1)
    return dag_file


@app.callback()
def main(debug: bool = False) -> None:
    """
    The main callback being called when option is provided after flow command. E.g. flow --debug run ...

    :params debug: whether to enable debug logs from third parties.
    """
    set_debug_mode(debug)


if __name__ == "__main__":  # pragma: no cover
    app()
