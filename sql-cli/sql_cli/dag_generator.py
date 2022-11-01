from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from networkx import DiGraph, depth_first_search, find_cycle, is_directed_acyclic_graph

from sql_cli.exceptions import DagCycle, EmptyDag, SqlFilesDirectoryNotFound
from sql_cli.sql_directory_parser import SqlFile, get_sql_files
from sql_cli.utils.jinja import render


@dataclass(frozen=True)
class SqlFilesDAG:
    """
    A DAG of sql files i.e. used for finding the right order to execute the sql files in.

    :param dag_id: The id of the DAG to generate.
    :param start_date: The start date of the DAG.
    :param sql_files: The sql files to use for DAG generation.
    """

    dag_id: str
    start_date: datetime
    sql_files: list[SqlFile]

    def __post_init__(self) -> None:
        if not self.sql_files:
            raise EmptyDag("Missing SQL files!")

    def has_sql_file(self, variable_name: str) -> bool:
        """
        Check whether the given variable name belongs to a real SQL file.

        :params variable_name: The variable name of the SQL file to check.

        :returns: True if there is any SQL file with the given variable name.
        """
        return any(sql_file.get_variable_name() == variable_name for sql_file in self.sql_files)

    def find_sql_file(self, variable_name: str) -> SqlFile:
        """
        Find a SQL file with the given variable name.

        :params variable_name: The variable name of the SQL file to find.

        :returns: if found a SQL file else raises an exception.
        """
        try:
            return next(
                sql_file for sql_file in self.sql_files if sql_file.get_variable_name() == variable_name
            )
        except StopIteration:
            raise ValueError("No sql file has been found for variable name!")

    def sorted_sql_files(self) -> list[SqlFile]:
        """
        Build, validate and sort the SQL files.

        :returns: a list of sql files sorted
            so they can be called sequentially in python code.
        """
        # Create a graph based on all sql files and parameters
        graph = DiGraph(
            [
                (sql_file, self.find_sql_file(parameter))
                for sql_file in self.sql_files
                for parameter in sql_file.get_parameters()
                if self.has_sql_file(parameter)
            ]
        )

        if not graph.nodes:
            # Add nodes without edges i.e. without any table references
            graph.add_nodes_from(self.sql_files)

        if not is_directed_acyclic_graph(graph):
            cycle_edges = " and ".join(
                " and ".join(edge.get_variable_name() for edge in edges) for edges in find_cycle(graph)
            )
            raise DagCycle(f"A cycle between {cycle_edges} has been detected!")

        return list(depth_first_search.dfs_postorder_nodes(graph))


def generate_dag(directory: Path, dags_directory: Path) -> Path:
    """
    Generate a DAG from SQL files.

    :params directory: The directory containing the raw sql files.
    :params dags_directory: The directory containing the generated DAG.

    :returns: the path to the DAG file.
    """
    if not directory.exists():
        raise SqlFilesDirectoryNotFound("The directory does not exist!")
    sql_files = sorted(get_sql_files(directory, target_directory=dags_directory))
    sql_files_dag = SqlFilesDAG(
        dag_id=directory.name,
        start_date=datetime(2020, 1, 1),
        sql_files=sql_files,
    )
    output_file = dags_directory / f"{sql_files_dag.dag_id}.py"
    render(
        template_file=Path("templates/dag.py.jinja2"),
        context={"dag": sql_files_dag},
        output_file=output_file,
    )
    return output_file
