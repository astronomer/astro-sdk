from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sql_cli.workflow_directory_parser import WorkflowFile, get_workflow_files

if TYPE_CHECKING:
    from astro.sql.operators.transform import TransformOperator  # pragma: no cover


def to_task_list(workflow_files: list[WorkflowFile]) -> list[TransformOperator]:
    """
    Converts the list of SQL Files into a list of TranformOperator tasks
    that all have proper dependencies set.
    :param workflow_files: The list of SQL files with necessary metadata for us to
        generate tasks with dependencies
    """
    param_dict = {s.name: s.to_operator() for s in workflow_files}
    for s in workflow_files:
        for p in s.get_parameters():
            if not param_dict.get(p):
                raise ValueError(f"variable '{p}' is undefined in file {s.name}")
            param_dict[s.name].parameters[p] = param_dict[p].output
            param_dict[s.name].set_upstream(param_dict[p])
    return list(param_dict.values())


def render_tasks(directory: Path) -> list[TransformOperator]:
    """
    render_dag allows a user to take any directory and turn it into a runnable
    Airflow DAG. This function will read all SQL files, and set dependencies based
    on jinja template-based variables.

    :param directory: Base directory for SQL files. We will recursively parse
        subdirectories as well.
    :param workflow_name: the name of the Workflow you would like to run
    :param start_date: (Optional) the start date you would like to set for your run.
        defaults to 2020-01-01
    :return: a DAG that can be run by any airflow
    """
    workflow_files: list[WorkflowFile] = sorted(get_workflow_files(directory, target_directory=None))
    return to_task_list(workflow_files)
