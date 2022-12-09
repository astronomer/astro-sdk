from __future__ import annotations

import os
from pathlib import Path
from typing import Any

from airflow.models import Connection

from sql_cli.constants import SQLITE_CONN_TYPE
from sql_cli.utils.rich import rprint

CONNECTION_ID_OUTPUT_STRING_WIDTH = 25


def convert_to_connection(conn: dict[str, Any], project_dir: Path | None) -> Connection:
    """
    Convert the SQL CLI connection dictionary into an Airflow Connection instance.

    :param conn: SQL CLI connection dictionary
    :param project_dir: Path to the initialised project
    :returns: Connection object
    """
    from airflow.api_connexion.schemas.connection_schema import connection_schema

    connection = conn.copy()
    connection["connection_id"] = connection["conn_id"]
    connection.pop("conn_id")
    if connection["conn_type"] == SQLITE_CONN_TYPE:
        host_path = connection["host"]
        if not os.path.isabs(host_path):
            # The example workflows have relative paths for the host URLs for SQLite connections. Additionally, the
            # user might also sometimes set relative paths for the host from the initialised project directory. Such
            # paths need to be converted to absolute paths so that the connections work successfully.
            resolved_host_path = project_dir / host_path
            if not resolved_host_path.exists():
                raise FileNotFoundError(
                    f"The relative file path {host_path} was resolved into {resolved_host_path} but it's a failed "
                    f"resolution as the path does not exist."
                )
            connection["host"] = str(resolved_host_path)
    return Connection(**connection_schema.load(connection))


def validate_connections(connections: list[Connection], connection_id: str | None = None) -> None:
    """
    Validates that the given connections are valid and registers them to Airflow with replace policy for existing
    connections.
    """
    if connection_id and not any(connection.conn_id == connection_id for connection in connections):
        rprint("[bold red]Error: Config file does not contain given connection[/bold red]", connection_id)

    for connection in connections:
        if not connection_id or connection_id and connection.conn_id == connection_id:
            os.environ[f"AIRFLOW_CONN_{connection.conn_id.upper()}"] = connection.get_uri()
            status = (
                "[bold green]PASSED[/bold green]" if _is_valid(connection) else "[bold red]FAILED[/bold red]"
            )
            rprint(f"Validating connection {connection.conn_id:{CONNECTION_ID_OUTPUT_STRING_WIDTH}}", status)


def _is_valid(connection: Connection) -> bool:
    # Sqlite automatically creates the file if it does not exist,
    # but our users might not expect that. They are referencing a database they expect to exist.
    if connection.conn_type == "sqlite" and not Path(connection.host).is_file():
        return False

    success_status, _ = connection.test_connection()
    return success_status
