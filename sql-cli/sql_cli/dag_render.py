from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from sql_cli.exceptions import SqlFilesDirectoryNotFound
from sql_cli.sql_directory_parser import SqlFile, get_sql_files

if TYPE_CHECKING:
    from astro.sql.operators.transform import TransformOperator


def to_transform_task_list(sql_files: list[SqlFile]) -> list[TransformOperator]:
    """
    Converts the list of SQL Files into a list of TranformOperator tasks
    that all have proper dependencies set.
    :return:
    """
    param_dict = {s.path.stem: s.to_transform_operator() for s in sql_files}
    for s in sql_files:
        for p in s.get_parameters():
            if not param_dict.get(p):
                raise ValueError(f"variable '{p}' is undefined in file {s.path.stem}")
            param_dict[s.path.stem].parameters[p] = param_dict[p].output
            param_dict[s.path.stem].set_upstream(param_dict[p])
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
    if not directory.exists():
        raise SqlFilesDirectoryNotFound("The directory does not exist!")
    sql_files: list[SqlFile] = sorted(get_sql_files(directory, target_directory=None))
    return to_transform_task_list(sql_files)
