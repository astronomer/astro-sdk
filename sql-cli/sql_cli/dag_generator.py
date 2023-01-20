from __future__ import annotations

from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
from pathlib import Path

import yaml

try:
    from airflow import Dataset
except ImportError:
    Dataset = None
from black import FileMode, format_str
from networkx import DiGraph, depth_first_search, find_cycle, is_directed_acyclic_graph

from sql_cli.constants import WORKFLOW_CONFIG_FILENAME
from sql_cli.exceptions import DagCycle, EmptyDag, WorkflowFilesDirectoryNotFound
from sql_cli.utils.airflow import dag_schedule_arg_name
from sql_cli.utils.jinja import render
from sql_cli.workflow_directory_parser import WorkflowFile, get_workflow_files

TEMPLATES_DIRECTORY = Path("templates")


def get_default_start_time() -> str:
    """
    Return the default start time for a workflow/DAG. At the moment, it is 24h in the past.

    :return: Datetime representing one day ago.
    """
    return (datetime.now().replace(microsecond=0) - timedelta(days=1)).isoformat()


@dataclass(frozen=True)
class Workflow:
    """
    A DAG of workflow files i.e. used for finding the right order to execute the files in.
    :param workflow_files: The workflow files to use for DAG generation.

    Also contains the following information which is used to create the Airflow DAG instance:
    :param catchup: Perform scheduler catchup (or only run latest)
    :param dag_id: The id of the DAG to generate
    :param description: The description for the DAG to e.g. be shown on the webserver
    :param is_paused_upon_creation: Specifies if the dag is paused when created for the first time.
        If the dag exists already, this flag will be ignored.
    :param orientation:  Specify DAG orientation in the UI graph view (LR, TB, RL, BT),
        default Left to Right (LR)
    :param schedule: Defines the rules according to which DAG runs are scheduled.
        Can accept cron string, timedelta or list of Dataset objects.
    :param start_date: The timestamp from which the scheduler will attempt to backfill
    :param end_date: A date beyond which your DAG won’t run, leave to None for open ended scheduling.
        None by default.
    :param tags: list[str] = ["SQL", "example"]

    Reference:
    https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/dag/index.html

    """

    workflow_files: list[WorkflowFile]

    # DAG attributes
    dag_id: str
    catchup: bool = False
    description: str | None = None
    is_paused_upon_creation: bool = False
    orientation: str = "LR"
    schedule: str | list[Dataset] | None = "@daily"
    start_date: datetime | str | None = None
    end_date: datetime | None = None
    tags: list[str] = field(default_factory=lambda: ["SQL"])

    schedule_arg_name: str = dag_schedule_arg_name()

    def __post_init__(self) -> None:
        if not self.workflow_files:
            raise EmptyDag("Missing workflow files!")

    def has_workflow_file(self, variable_name: str) -> bool:
        """
        Check whether the given variable name belongs to a real workflow file.

        :param variable_name: The variable name of the workflow file to check.

        :return: True if there is any workflow file with the given variable name.
        """
        return any(
            workflow_file.get_variable_name() == variable_name for workflow_file in self.workflow_files
        )

    def find_workflow_file(self, variable_name: str) -> WorkflowFile:
        """
        Find a SQL file with the given variable name.

        :param variable_name: The variable name of the workflow file to find.

        :return: if found a workflow file else raises an exception.
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

        :return: a list of sql files sorted
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

    @classmethod
    def from_yaml(
        cls, yaml_path: Path, dag_id: str | None = None, workflow_files: list[WorkflowFile] | None = None
    ) -> Workflow:
        """
        Create a Workflow data class loading properties from a YAML file

        :param yaml_path: path to YAML file configuration
        :param dag_id: (optional) used to define the Workflow dag_id, it's a fallback if not defined in the YAML

        :return: Workflow instance with the properties declared in the YAML file
        """

        class DatasetYAML(yaml.YAMLObject):  # skipcq: PYL-W0612, PTC-W0065
            """
            Used to parse Dataset information from YML file.
            """

            yaml_tag = "!dataset"

            def __init__(self, uri: str) -> None:
                self.uri = uri

        yaml.add_path_resolver("!dataset", ["DatasetYAML"], dict)

        default_params = {
            "dag_id": dag_id,
            "workflow_files": workflow_files or [],
            "start_date": get_default_start_time(),
        }
        with yaml_path.open() as fp:
            yaml_params = yaml.safe_load(fp.read()).get("workflow", {})
            yaml_params = {**default_params, **yaml_params}

        workflow_fields = tuple(field.name for field in fields(Workflow))
        workflow_params = {key: value for key, value in yaml_params.items() if key in workflow_fields}
        return Workflow(**workflow_params)


def generate_dag(directory: Path, dags_directory: Path, generate_tasks: bool) -> Path:
    """
    Generate a DAG from SQL files.

    :param directory: The directory containing the raw workflow files.
    :param dags_directory: The directory containing the generated DAG.
    :param generate_tasks: Whether the user wants to explicitly generate each task
        of the airflow DAG or rely on a less verbose `render` function.

    :return: the path to the DAG file.
    """
    if not directory.exists():
        raise WorkflowFilesDirectoryNotFound("The directory does not exist!")

    workflow_files = sorted(get_workflow_files(directory, target_directory=dags_directory))
    workflow_config_file = directory / WORKFLOW_CONFIG_FILENAME
    if workflow_config_file.exists():
        workflow_files_dag = Workflow.from_yaml(
            directory / WORKFLOW_CONFIG_FILENAME,
            dag_id=directory.name,
            workflow_files=workflow_files,
        )
    else:
        workflow_files_dag = Workflow(
            dag_id=directory.name,
            start_date=get_default_start_time(),
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
        context={"dag": workflow_files_dag, "schedule_type": type(workflow_files_dag.schedule).__name__},
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
