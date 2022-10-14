import logging
from pathlib import Path
from typing import Optional

import typer
from rich import print as rprint

import sql_cli
from sql_cli import configuration, project
from sql_cli.connections import validate_connections
from sql_cli.dag_generator import generate_dag

app = typer.Typer(add_completion=False)

for name in logging.root.manager.loggerDict:
    logging.getLogger(name).setLevel(logging.ERROR)


@app.command()
def version() -> None:
    """
    Print the SQL CLI version.
    """
    rprint("Astro SQL CLI", sql_cli.__version__)


@app.command()
def about() -> None:
    """
    Print additional information about the project.
    """
    rprint("Find out more: https://github.com/astronomer/astro-sdk/sql-cli")


@app.command()
def generate(directory: Path, dags_directory: Path) -> None:
    """
    Generate the Airflow DAG from a directory of SQL files.

    :params directory: The directory containing the raw sql files.
    :params dags_directory: The directory containing the generated DAG.
    """
    dag_file = generate_dag(directory, dags_directory)
    rprint("The DAG file", dag_file.resolve(), "has been successfully generated. 🎉")


@app.command()
def validate(environment: str = "default", connection: Optional[str] = None) -> None:
    """Validate Airflow connection(s) provided in the configuration file for the given environment"""
    validate_connections(environment=environment, connection_id=connection)


@app.command()
def init(
    project_dir: Optional[Path] = typer.Argument(
        None, dir_okay=True, metavar="PATH", help="(Optional) Default: current directory.", show_default=False
    ),
    airflow_home: Optional[Path] = typer.Option(
        None,
        dir_okay=True,
        help=f"(Optional) Set the Airflow Home. Default: {configuration.DEFAULT_AIRFLOW_HOME}",
        show_default=False,
    ),
    airflow_dags_folder: Optional[Path] = typer.Option(
        None,
        dir_okay=True,
        help=f"(Optional) Set the DAGs Folder. Default: {configuration.DEFAULT_DAGS_FOLDER}",
        show_default=False,
    ),
) -> None:
    """
    Initialise a project structure to write workflows using SQL files.

    \b\n
    Examples of usage:
    \b\n
    $ flow init
    \b\n
    $ flow init .
    \b\n
    $ flow init project_name


    \b\n
    By default, the project structure includes:

    ├── config: withholds configuration, e.g. database connections, within each environment directory
    \b\n
    ├── data: directory which contains datasets, including SQLite databases used by the examples
    \b\n
    └── workflows: directory where SQL workflows are declared, by default has two examples of workflow

    \b\n
    Next steps:
    \b\n
    * Update the file `config/default/configuration.yaml` to declare database connections.
    \b\n
    * Create SQL workflows within the `workflows` folder.
    """
    project_dir = project_dir or Path.cwd()

    proj = project.Project(project_dir, airflow_home, airflow_dags_folder)
    proj.initialise()
    rprint("Initialized an Astro SQL project at", project_dir)


if __name__ == "__main__":  # pragma: no cover
    app()
