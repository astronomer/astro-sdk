class DagCycle(Exception):
    """An exception raised when DAG contains a cycle."""


class InvalidProject(Exception):
    """An exception raised when Project is invalid."""


class EmptyDag(Exception):
    """An exception raised when there are no SQL files within the DAG."""


class SqlFilesDirectoryNotFound(Exception):
    """An exception raised when the sql files directory does not exist."""
