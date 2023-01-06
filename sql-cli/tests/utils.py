from __future__ import annotations

import random
import string
from pathlib import Path

from airflow.models import Connection

from astro.table import MAX_TABLE_NAME_LENGTH


def list_dir(path: Path) -> list[Path]:
    """
    Return sorted list of files and directories available in the given directory.

    :param path: Path to the directory

    :returns: Sorted list of files and directories within the given directory.
    """
    return sorted(p.relative_to(path) for p in path.rglob("*"))


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


def create_unique_table_name(length: int = MAX_TABLE_NAME_LENGTH) -> str:
    """
    Create a unique table name of the requested size, which is compatible with all supported databases.

    :return: Unique table name
    :rtype: str
    """
    unique_id = random.choice(string.ascii_lowercase) + "".join(
        random.choice(string.ascii_lowercase + string.digits) for _ in range(length - 1)
    )
    return unique_id
