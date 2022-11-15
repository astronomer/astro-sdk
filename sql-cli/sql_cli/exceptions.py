import warnings

try:
    from astro.exceptions import ConnectionFailed
except ImportError:

    class ConnectionFailed(Exception):  # type: ignore
        """An exception raised when the sql file's connection cannot be established."""

        def __init__(self, *args: object, conn_id: str) -> None:
            warnings.warn(
                "Deprecation Warning: We will switch to astro.exceptions.ConnectionFailed as of astro-python-sdk 1.3"
            )
            self.conn_id = conn_id
            super().__init__(*args)


class DagCycle(Exception):
    """An exception raised when DAG contains a cycle."""


class InvalidProject(Exception):
    """An exception raised when Project is invalid."""


class EmptyDag(Exception):
    """An exception raised when there are no SQL files within the DAG."""


class SqlFilesDirectoryNotFound(Exception):
    """An exception raised when the sql files directory does not exist."""
