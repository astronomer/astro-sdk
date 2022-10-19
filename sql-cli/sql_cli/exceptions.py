class DagCycle(Exception):
    """An exception raised when DAG contains a cycle."""


class InvalidProject(Exception):
    """An exception raised when Project is invalid."""
