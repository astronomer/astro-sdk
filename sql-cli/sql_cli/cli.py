from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from airflow.models import DAG  # pragma: no cover

from rich import print as rprint
from typer import Exit

from sql_cli import dag_generator, run_dag as dag_runner
from sql_cli.exceptions import ConnectionFailed, DagCycle, EmptyDag, SqlFilesDirectoryNotFound
from sql_cli.project import Project
from airflow.utils.state import State
from airflow.models.dagbag import DagBag
from airflow.utils.cli import process_subdir


def generate_dag(project: Project, env: str, workflow_name: str, gen_dag: bool = False) -> Path:
    rprint(
        f"\nGenerating the DAG file from workflow [bold blue]{workflow_name}[/bold blue]"
        f" for [bold]{env}[/bold] environment..\n"
    )
    try:
        if not gen_dag:
            dag_file = dag_generator.generate_render_dag(
                directory=project.directory / project.workflows_directory / workflow_name,
                dags_directory=project.airflow_dags_folder,
            )
        else:
            dag_file = dag_generator.generate_dag(
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
    rprint("The DAG file", dag_file.resolve(), "has been successfully generated. 🎉")
    import_errors = DagBag(process_subdir(str(dag_file))).import_errors
    if import_errors:
        all_errors = "\n\n".join(list(import_errors.values()))
        rprint(f"[bold red]Workflow failed to render[/bold red]\n errors found:\n\n {all_errors}")
        raise Exit(code=1)
    return dag_file


def run_dag(project: Project, env: str, dag: DAG, verbose: bool) -> None:
    rprint(
        f"\nRunning the workflow [bold blue]{dag.dag_id}[/bold blue] for [bold]{env}[/bold] environment..\n"
    )
    try:
        dr = dag_runner.run_dag(
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
    rprint(f"Completed running the workflow {dr.dag_id}. 🚀")
    final_state = None
    if dr.state == State.SUCCESS:
        final_state = "[bold green]SUCCESS[/bold green]"
    elif final_state == State.FAILED:
        final_state = "[bold red]FAILED[/bold red]"
    else:
        final_state = dr.state
    rprint(f"Final state: {final_state}")
    elapsed_seconds = (dr.end_date - dr.start_date).microseconds / 10**6
    rprint(f"Total elapsed time: [bold blue]{elapsed_seconds:.2}s[/bold blue]")

    elapsed_seconds = (dr.end_date - dr.start_date).microseconds / 10**6
    rprint(f"Total elapsed time: [bold blue]{elapsed_seconds:.2}s[/bold blue]")
