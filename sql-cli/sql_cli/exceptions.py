class DagCycle(Exception):
    """An exception raised when DAG contains a cycle."""


class InvalidProject(Exception):
    """An exception raised when Project is invalid."""


class EmptyDag(Exception):
    """An exception raised when there are no SQL files within the DAG."""


class WorkflowFilesDirectoryNotFound(Exception):
    """An exception raised when the workflow files directory does not exist."""


class ConnectionFailed(Exception):
    """An exception raised when the sql file's connection cannot be established."""

    def __init__(self, *args: object, conn_id: str) -> None:
        self.conn_id = conn_id
        super().__init__(*args)
