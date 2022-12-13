from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

from astro.render.workflow_directory_parser import WorkflowFile, get_workflow_files

if TYPE_CHECKING:
    from astro.sql.operators.transform import TransformOperator  # pragma: no cover


def to_task_list(workflow_files: list[WorkflowFile]) -> list[TransformOperator]:
    """
    Converts the list of SQL Files into a list of TransformOperator tasks
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

    The render function allows users to create a directory of SQL files and YAML files that reference each other.
    This function will parse those files and turn them into Airflow tasks that reference each other using jinja
    variables

    For example, assuming that the directory contains the following two SQL files:

    a.sql
    .. code-block:: sql

        ---
        conn_id: my_test_sqlite
        ---
        select 1

    b.sql
    .. code-block:: sql

        select * from {{ a }}


    The render function will parse these files and create Airflow tasks that reference each other.
    The b task will depend on the a task, and the a task will use the my_test_sqlite connection.
    The resulting list of tasks will be returned by the render function as a dictionary of {"file name": Operator}.

    Users may also use a YAML file to run the `load_file` function like this:

    load_data.yaml
    .. code-block:: yaml

        load_file:
            input_file:
                path: "https://raw.githubusercontent.com/astronomer/astro-sdk/main/tests/data/imdb.csv"
        output_table:
            conn_id: "sqlite_conn"

    transform.sql
    .. code-block:: sql

        SELECT * FROM {{ load_data }}

    :param directory: Base directory for SQL files. We will recursively parse
        subdirectories as well.
    """
    workflow_files: list[WorkflowFile] = sorted(get_workflow_files(directory, target_directory=None))
    return to_task_list(workflow_files)
