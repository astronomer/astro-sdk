import logging
from pathlib import Path

import typer
from airflow.utils.cli import get_dag
from dotenv import load_dotenv
from rich import print as rprint
from typer import Typer

from sql_cli import __version__
from sql_cli.connections import validate_connections
from sql_cli.dag_generator import generate_dag
from sql_cli.run_dag import run_dag

load_dotenv()
app = Typer(add_completion=False)

for name in logging.root.manager.loggerDict:
    logging.getLogger(name).setLevel(logging.ERROR)


@app.command(help="Print the SQL CLI version.")
def version() -> None:
    rprint("Astro SQL CLI", __version__)


@app.command(help="Print additional information about the project.")
def about() -> None:
    rprint("Find out more: https://github.com/astronomer/astro-sdk/sql-cli")


@app.command(help="Generate the Airflow DAG from a directory of SQL files.")
def generate(
    directory: Path = typer.Argument(
        default=...,
        show_default=False,
        exists=True,
        help="directory containing the sql files.",
    ),
    dags_directory: Path = typer.Argument(
        default=...,
        show_default=False,
        exists=True,
        help="directory for the generated DAGs.",
    ),
) -> None:
    dag_file = generate_dag(directory, dags_directory)
    rprint("The DAG file", dag_file.resolve(), "has been successfully generated. 🎉")


@app.command(
    help="Validate Airflow connection(s) provided in the configuration file for the given environment."
)
def validate(
    environment: str = typer.Argument(
        default="default",
        help="environment to validate",
    ),
    connection: str = typer.Argument(
        default=None,
        help="airflow connection id to validate",
    ),
) -> None:
    validate_connections(environment=environment, connection_id=connection)


@app.command(
    help="""
    Run a workflow locally. This task assumes that there is a local airflow DB (can be a SQLite file), that has been
    initialized with Airflow tables. Users can also add paths to a connections or variable yaml file which will override
    existing connections for the test run.

    \b
    Example of a connections.yaml file:

    \b
    my_sqlite_conn:
        conn_id: my_sqlite_conn
        conn_type: sqlite
        host: ...
    my_postgres_conn:
        conn_id: my_postgres_conn
        conn_type: postgres
        ...
    """
)
def run(
    directory: Path = typer.Argument(
        default=...,
        show_default=False,
        exists=True,
        help="directory containing the sql files.",
    ),
    dags_directory: Path = typer.Argument(
        default=...,
        show_default=False,
        exists=True,
        help="directory for the generated DAGs.",
    ),
    connection_file: Path = typer.Option(
        default=None,
        exists=True,
        help="path to connections yaml or json file",
    ),
    variable_file: Path = typer.Option(
        default=None,
        exists=True,
        help="path to variables yaml or json file",
    ),
) -> None:
    dag_file = generate_dag(directory, dags_directory=dags_directory)
    dag = get_dag(dag_id=directory.name, subdir=dag_file.parent.as_posix())
    run_dag(
        dag=dag,
        conn_file_path=connection_file.as_posix() if connection_file else None,
        variable_file_path=variable_file.as_posix() if variable_file else None,
    )


if __name__ == "__main__":  # pragma: no cover
    app()
