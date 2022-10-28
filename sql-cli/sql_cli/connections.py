from __future__ import annotations

from pathlib import Path

from airflow.models import Connection
from airflow.utils.session import create_session
from rich import print as rprint

CONNECTION_ID_OUTPUT_STRING_WIDTH = 25


def _create_or_replace_connection(conn_obj: Connection) -> None:
    """Creates a new or replaces existing connection in the Airflow DB with the given connection object."""
    conn_id = conn_obj.conn_id
    with create_session() as session:
        db_connection = session.query(Connection).filter_by(conn_id=conn_id).one_or_none()
        if db_connection:
            session.delete(db_connection)
            session.commit()
        session.add(conn_obj)
        session.commit()


def validate_connections(connections: list[Connection], connection_id: str | None = None) -> None:
    """
    Validates that the given connections are valid and registers them to Airflow with replace policy for existing
    connections.
    """
    config_file_contains_connection = False

    for connection in connections:
        if connection.id == connection_id:
            config_file_contains_connection = True
        _create_or_replace_connection(connection)
        status = "[bold green]PASSED[/bold green]" if _is_valid(connection) else "[bold red]FAILED[/bold red]"
        rprint(f"Validating connection {connection.conn_id:{CONNECTION_ID_OUTPUT_STRING_WIDTH}}", status)

    if not config_file_contains_connection:
        rprint("Error: Config file does not contain given connection", connection_id)


def _is_valid(connection: Connection) -> bool:
    # Sqlite automatically creates the file if it does not exist,
    # but our users might not expect that. They are referencing a database they expect to exist.
    if connection.conn_type == "sqlite" and not Path(connection.host).is_file():
        return False

    success_status, _ = connection.test_connection()
    return success_status
