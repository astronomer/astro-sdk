from __future__ import annotations

from pathlib import Path

from airflow.models import Connection


def list_dir(dir_name: str) -> list[Path]:
    """
    Return sorted list of files and directories available in the given directory.

    :param dir_name: Source directory name

    :returns: Sorted list of files and directories within the given directory.
    """
    return sorted([path.relative_to(dir_name) for path in Path(dir_name).rglob("*")])


def get_connection_by_id(connections: list[Connection], connection_id: str) -> Connection | None:
    """
    Get a connection by id.

    :param connections: The connections to iterate through.
    :param connection_id: The id of the connection to look for.

    :returns: the connection object of the id if exists.
    """
    for connection in connections:
        if connection.conn_id == connection_id:
            return connection
    return None
