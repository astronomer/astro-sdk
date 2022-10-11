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
def init(target_dir: Optional[str] = typer.Argument(None)) -> None:
    """
    Initialises a SQL CLI project.
    """
    if target_dir is None:
        target_dir = os.getcwd()

    Project.initialise(Path(target_dir))
    rprint(f"Initialized an Astro SQL project at {target_dir}")


if __name__ == "__main__":  # pragma: no cover
    for name in logging.root.manager.loggerDict:
        logging.getLogger(name).setLevel(logging.ERROR)

   app()
