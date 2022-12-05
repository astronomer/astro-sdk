from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from black import FileMode, format_str
from networkx import DiGraph, depth_first_search, find_cycle, is_directed_acyclic_graph

from sql_cli.exceptions import DagCycle, EmptyDag, WorkflowFilesDirectoryNotFound
from sql_cli.utils.jinja import render
from sql_cli.workflow_directory_parser import WorkflowFile, get_workflow_files

TEMPLATES_DIRECTORY = Path("templates")


@dataclass(frozen=True)
class Workflow:
    """
    A DAG of workflow files i.e. used for finding the right order to execute the files in.

    :param dag_id: The id of the DAG to generate.
    :param start_date: The start date of the DAG.
    :param workflow_files: The workflow files to use for DAG generation.
    """

    dag_id: str
    start_date: datetime
    workflow_files: list[WorkflowFile]

    def __post_init__(self) -> None:
        if not self.workflow_files:
            raise EmptyDag("Missing workflow files!")

    def has_workflow_file(self, variable_name: str) -> bool:
        """
        Check whether the given variable name belongs to a real workflow file.

        :params variable_name: The variable name of the workflow file to check.

        :returns: True if there is any workflow file with the given variable name.
        """
        return any(
            workflow_file.get_variable_name() == variable_name for workflow_file in self.workflow_files
        )

    def find_workflow_file(self, variable_name: str) -> WorkflowFile:
        """
        Find a SQL file with the given variable name.

        :params variable_name: The variable name of the workflow file to find.

        :returns: if found a workflow file else raises an exception.
        """
        try:
            return next(
                workflow_file
                for workflow_file in self.workflow_files
                if workflow_file.get_variable_name() == variable_name
            )
        except StopIteration:
            raise ValueError("No workflow file has been found for variable name!")

    def sorted_workflow_files(self) -> list[WorkflowFile]:
        """
        Build, validate and sort the SQL files.

        :returns: a list of sql files sorted
            so they can be called sequentially in python code.
        """
        # Create a graph based on all sql files and parameters
        graph = DiGraph(
            [
                (workflow_file, self.find_workflow_file(parameter))
                for workflow_file in self.workflow_files
                for parameter in workflow_file.get_parameters()
                if self.has_workflow_file(parameter)
            ]
        )

        if not graph.nodes:
            # Add nodes without edges i.e. without any table references
            graph.add_nodes_from(self.workflow_files)

        if not is_directed_acyclic_graph(graph):
            cycle_edges = " and ".join(
                " and ".join(edge.get_variable_name() for edge in edges) for edges in find_cycle(graph)
            )
            raise DagCycle(f"A cycle between {cycle_edges} has been detected!")

        return list(depth_first_search.dfs_postorder_nodes(graph))


def generate_dag(directory: Path, dags_directory: Path, generate_tasks: bool) -> Path:
    """
    Generate a DAG from SQL files.

    :params directory: The directory containing the raw workflow files.
    :params dags_directory: The directory containing the generated DAG.
    :params generate_tasks: Whether the user wants to explicitly generate each task
        of the airflow DAG or rely on a less verbose `render` function.

    :returns: the path to the DAG file.
    """
    if not directory.exists():
        raise WorkflowFilesDirectoryNotFound("The directory does not exist!")
    workflow_files = sorted(get_workflow_files(directory, target_directory=dags_directory))
    workflow_files_dag = Workflow(
        dag_id=directory.name,
        start_date=datetime(2020, 1, 1),
        workflow_files=workflow_files,
    )
    if generate_tasks:
        template_file = TEMPLATES_DIRECTORY / "gen_tasks_dag.py.jinja2"
    else:
        for workflow_file in workflow_files_dag.sorted_workflow_files():
            workflow_file.write_raw_content_to_target_path()
        template_file = TEMPLATES_DIRECTORY / "render_dag.py.jinja2"

    output_file = dags_directory / f"{workflow_files_dag.dag_id}.py"
    render(
        template_file=template_file,
        context={"dag": workflow_files_dag},
        output_file=output_file,
    )

    _black_format_generated_file(output_file)
    return output_file


def _black_format_generated_file(output_file: Path) -> None:
    with output_file.open("r+") as file:
        output_str = format_str(file.read(), mode=FileMode())
        file.seek(0)
        file.write(output_str)
        file.truncate()
