from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import DAG, DagRun  # pragma: no cover

from rich import print as rprint
from typer import Exit

from sql_cli import dag_generator, run_dag as dag_runner
from sql_cli.exceptions import ConnectionFailed, DagCycle, EmptyDag, SqlFilesDirectoryNotFound
from sql_cli.project import Project


def generate_dag(project: Project, workflow_name: str) -> Path:
    try:
        return dag_generator.generate_dag(
            directory=project.directory / project.workflows_directory / workflow_name,
            dags_directory=project.airflow_dags_folder,
        )
    except EmptyDag:
        rprint(f"[bold red]The workflow {workflow_name} does not have any SQL files![/bold red]")
        raise Exit(code=1)
    except SqlFilesDirectoryNotFound:
        rprint(f"[bold red]The workflow {workflow_name} does not exist![/bold red]")
        raise Exit(code=1)
    except DagCycle as dag_cycle:
        rprint(f"[bold red]The workflow {workflow_name} contains a cycle! {dag_cycle}[/bold red]")
        raise Exit(code=1)


def run_dag(project: Project, dag: DAG, verbose: bool) -> DagRun:
    try:
        return dag_runner.run_dag(
            dag,
            run_conf=project.airflow_config,
            connections={c.conn_id: c for c in project.connections},
            verbose=verbose,
        )
    except ConnectionFailed as connection_failed:
        rprint(
            f"  [bold red]{connection_failed}[/bold red] using connection [bold]{connection_failed.conn_id}[/bold]"
        )
        raise Exit(code=1)
    except Exception as exception:
        rprint(f"  [bold red]{exception}[/bold red]")
        raise Exit(code=1)
