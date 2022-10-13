import logging
import os
import shutil
from pathlib import Path
from typing import Optional

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


@app.command()
def version() -> None:
    """
    Print the SQL CLI version.
    """
    rprint("Astro SQL CLI", __version__)


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
def run(
    dag_id: str = typer.Argument("dag_id", help="ID of the DAG you want to run", show_default=False),
    subdir: str = typer.Argument(
        "subdir",
        help="the subdirectory or filepath for the DAG. "
        "The more precise this is the faster the local runner will run as we won't need to parse as many DAGs",
        show_default=False,
    ),
    dags_directory: Optional[str] = typer.Option(help="path to connections yaml or json file", default=None),
    conn_file_path: Optional[str] = typer.Option(help="path to connections yaml or json file", default=None),
    variable_file_path: Optional[str] = typer.Option(
        help="path to variables yaml or json file", default=None
    ),
    is_sql: bool = typer.Option(help="Whether this DAG is a sql_cli directory", default=True),
) -> None:
    """
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


    :param dags_directory:
    :param dag_id: ID of the DAG you want to run
    :param subdir: the subdirectory or filepath for the DAG. The more precise this is the faster the local runner
    will run as we won't need to parse as many DAGs
    :param conn_file_path: path to connections yaml or json file
    :param variable_file_path: path to variables yaml or json file
    :param is_sql:
    """
    if is_sql:
        generate_dag(Path(subdir), dags_directory=Path(dags_directory))  # type: ignore

    dag = get_dag(dag_id=dag_id, subdir=dags_directory or subdir)
    run_dag(dag=dag, conn_file_path=conn_file_path, variable_file_path=variable_file_path)


def generate_temp_dag(root_directory, tmp_dir):
    shutil.copytree(root_directory, tmp_dir + "/root")
    os.mkdir(Path(tmp_dir) / "target")
    os.mkdir(Path(tmp_dir) / "dag")
    print(tmp_dir)
    return generate_dag(Path(tmp_dir) / "root", dags_directory=Path(tmp_dir) / "dag")


if __name__ == "__main__":  # pragma: no cover
    app()
