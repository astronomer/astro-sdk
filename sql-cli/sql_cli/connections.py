from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

from airflow.models import Connection

from sql_cli.constants import LOGGER_NAME
from sql_cli.utils.rich import rprint

CONNECTION_ID_OUTPUT_STRING_WIDTH = 25

log = logging.getLogger(LOGGER_NAME)


def convert_to_connection(conn: dict[str, Any], data_dir: Path) -> Connection:
    """
    Convert the SQL CLI connection dictionary into an Airflow Connection instance.

    :param conn: SQL CLI connection dictionary
    :param data_dir: Path to the initialised project's data directory
    :returns: Connection object
    """
    from airflow.api_connexion.schemas.connection_schema import connection_schema

    connection = conn.copy()
    connection["connection_id"] = connection.pop("conn_id")

    if connection["conn_type"] == "sqlite" and not os.path.isabs(connection["host"]):
        # Try resolving with data directory
        resolved_host = data_dir / connection["host"]
        if not resolved_host.is_file():
            log.error(
                "The relative file path %s was resolved into %s but it does not exist.",
                connection["host"],
                resolved_host,
            )
        connection["host"] = resolved_host.as_posix()

    connection_kwargs = connection_schema.load(connection)
    return Connection(**connection_kwargs)


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
            success_status, message = _is_valid(connection)
            status = "[bold green]PASSED[/bold green]" if success_status else "[bold red]FAILED[/bold red]"
            rprint(f"Validating connection {connection.conn_id:{CONNECTION_ID_OUTPUT_STRING_WIDTH}}", status)
            if not success_status:
                rprint(f"  [bold red]Error: {message}[/bold red]")
            formatted_message = (
                f"[bold green]{message}[/bold green]" if success_status else f"[bold red]{message}[/bold red]"
            )
            log.debug("Connection Message: %s", formatted_message)


def _is_valid(connection: Connection) -> tuple[bool, str]:
    # Sqlite automatically creates the file if it does not exist,
    # but our users might not expect that. They are referencing a database they expect to exist.
    if connection.conn_type == "sqlite" and not Path(connection.host).is_file():
        return False, "Sqlite db does not exist!"

    return connection.test_connection()
