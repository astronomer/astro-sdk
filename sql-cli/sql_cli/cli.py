from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Dict

if TYPE_CHECKING:
    from airflow.models import DAG  # pragma: no cover

from airflow.models.dagbag import DagBag
from airflow.utils.cli import process_subdir
from rich import print as rprint
from airflow.models.dagrun import DagRun
from sql_cli import dag_generator, run_dag as dag_runner
from sql_cli.project import Project


def generate_dag(project: Project, env: str, workflow_name: str, generate_tasks: bool = False) -> Path:
    dag_file = dag_generator.generate_dag(
        directory=project.directory / project.workflows_directory / workflow_name,
        dags_directory=project.airflow_dags_folder,
        generate_tasks=generate_tasks,
    )


def check_for_dag_import_errors(dag_file: Path) -> Dict[str,str]:
    return DagBag(process_subdir(str(dag_file))).import_errors


def run_dag(project: Project, dag: DAG, verbose: bool) -> DagRun:
    return dag_runner.run_dag(
        dag,
        run_conf=project.airflow_config,
        connections={c.conn_id: c for c in project.connections},
        verbose=verbose,
    )

