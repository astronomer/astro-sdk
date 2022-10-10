from pathlib import Path

from rich import print as rprint
from typer import Typer

from sql_cli import __version__
from sql_cli.dag_generator import generate_dag

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


if __name__ == "__main__":  # pragma: no cover
    app()
