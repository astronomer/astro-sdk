from __future__ import annotations

from networkx import DiGraph, depth_first_search, find_cycle, is_directed_acyclic_graph
from sql_cli.sql_directory_parser import SqlFile


class DagCycle(Exception):
    """An exception raised when DAG contains a cycle."""


class SqlFilesDAG:
    """
    A DAG of sql files i.e. used for finding the right order to execute the sql files in.

    :param sql_files: The sql files to use for DAG generation.
    """

    def __init__(self, sql_files: set[SqlFile]) -> None:
        self.sql_files = sorted(sql_files)
        self.nodes = [sql_file.get_variable_name() for sql_file in self.sql_files]
        self.edges = [
            (sql_file.get_variable_name(), parameter)
            for sql_file in self.sql_files
            for parameter in sql_file.get_parameters()
            if parameter in self.nodes  # only add edges for existing nodes
        ]

    def build(self) -> list[SqlFile]:
        """
        Build a directed graph from a list of sql files.

        :returns: a list of sql files sorted by least dependencies first
            so they can be called sequentially in python code.
        """
        graph = DiGraph()
        graph.add_nodes_from(self.nodes)
        graph.add_edges_from(self.edges)

        if not is_directed_acyclic_graph(graph):
            raise DagCycle(
                "Could not generate DAG! "
                f"A cycle between {' and '.join(' and '.join(edges) for edges in find_cycle(graph))} has been detected!"
            )

        order = list(depth_first_search.dfs_postorder_nodes(graph))

        return sorted(
            self.sql_files,
            key=lambda sql_file: order.index(sql_file.get_variable_name()),
        )
