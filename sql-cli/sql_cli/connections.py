from __future__ import annotations

import os
from pathlib import Path

from airflow.models import Connection

from sql_cli.utils.rich import rprint

CONNECTION_ID_OUTPUT_STRING_WIDTH = 25


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
