import logging
import os
from pathlib import Path
from typing import Optional

import typer
from rich import print as rprint

from sql_cli import __version__
from sql_cli.connections import validate_connections
from sql_cli.dag_generator import generate_dag
from sql_cli.project import Project

app = typer.Typer(add_completion=False)


@app.command()
def version() -> None:
    """
    Print the SQL CLI version.
    """
    rprint(f"Astro SQL CLI {sql_cli.__version__}")


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
def init(project_dir: Optional[str] = typer.Argument(None)) -> None:
    """
    Initialise a SQL CLI project structure.

    By default, this includes:

    \b\n
    ├── config
    \b\n
    │   ├── default
    \b\n
    │   │   └── configuration.yml
    \b\n
    │   └── dev
    \b\n
    │       └── configuration.yml
    \b\n
    ├── data
    \b\n
    │   ├── movies.db
    \b\n
    │   └── retail.db
    \b\n
    └── workflows
    \b\n
    ├── example_basic_transform
    \b\n
    │   └── top_animations.sql
    \b\n
    └── example_templating
    \b\n
        ├── filtered_orders.sql
    \b\n
        └── joint_orders_customers.sql

    \b\n
    Update the file `config/default/configuration.yaml` to declare your databases.
    \b\n
    Create SQL workflows within the `workflows` folder.
    """
    if project_dir is None:
        project_dir = os.getcwd()

    Project.initialise(Path(project_dir))
    rprint(f"Initialized an Astro SQL project at {project_dir}")


if __name__ == "__main__":  # pragma: no cover
    for name in logging.root.manager.loggerDict:
        logging.getLogger(name).setLevel(logging.ERROR)

   app()
