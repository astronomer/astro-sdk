from pathlib import Path
from typing import Optional

from airflow.utils.cli import get_dag
from rich import print as rprint
from typer import Typer

from sql_cli import __version__
from sql_cli.dag_generator import generate_dag
from sql_cli.run_dag import run_dag

app = Typer(add_completion=False)


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
def generate(
    directory: Path,
    target_directory: Path,
    dags_directory: Path,
) -> None:
    """
    Generate the Airflow DAG from a directory of SQL files.
    """
    dag_file = generate_dag(directory, target_directory, dags_directory)
    rprint("The DAG file", dag_file.resolve(), "has been successfully generated. 🎉")


@app.command()
def run(
    dag_id: str, subdir: str, conn_file_path: Optional[str] = None, variable_file_path: Optional[str] = None
) -> None:
    """
    Run a DAG locally. This task assumes that there is a local airflow DB (can be a SQLite file), that has been
    initialized with Airflow tables. Users can also add paths to a connections or variable yaml file which will override
    existing connections for the test run.

    Example of a connections.yaml file:

    ```
    my_sqlite_conn:
        conn_id: my_sqlite_conn
        conn_type: sqlite
        host: ...
    my_postgres_conn:
        conn_id: my_postgres_conn
        conn_type: postgres
        ...
    ```

    :param dag_id: ID of the DAG you want to run
    :param subdir: the subdirectory or filepath for the DAG. The more precise this is the faster the local runner
    will run as we won't need to parse as many DAGs
    :param conn_file_path: path to connections yaml or json file
    :param variable_file_path: path to variables yaml or json file
    """
    dag = get_dag(dag_id=dag_id, subdir=subdir)
    run_dag(dag=dag, conn_file_path=conn_file_path, variable_file_path=variable_file_path)


if __name__ == "__main__":  # pragma: no cover
    app()
