from typing import Optional

import typer
from airflow.utils.cli import get_dag

import sql_cli
from sql_cli.run_dag import run_dag

app = typer.Typer(add_completion=False)


@app.command()
def version() -> None:
    """
    Print the SQL CLI version.
    """
    typer.echo(f"Astro SQL CLI {sql_cli.__version__}")


@app.command()
def about() -> None:
    """
    Print additional information about the project.
    """
    typer.echo("Find out more: https://github.com/astronomer/astro-sdk/sql-cli")


@app.command()
def run(
    dag_id: str, subdir: str, conn_file_path: Optional[str] = None, variable_file_path: Optional[str] = None
) -> None:
    dag = get_dag(dag_id=dag_id, subdir=subdir)
    run_dag(dag=dag, conn_file_path=conn_file_path, variable_file_path=variable_file_path)


if __name__ == "__main__":  # pragma: no cover
    app()
