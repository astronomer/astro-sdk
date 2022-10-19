from __future__ import annotations

from typing import Any

from airflow.api_connexion.schemas.connection_schema import connection_schema
from airflow.models import Connection
from airflow.utils.session import create_session
from rich import print as pprint

from sql_cli.project import Project
from sql_cli.utils.airflow import retrieve_airflow_database_conn_from_config, set_airflow_database_conn

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


def convert_to_connection(conn: dict[str, Any]) -> Connection:
    """
    Convert the SQL CLI connection dictionary into an Airflow Connection instance.

    :param conn: SQL CLI connection dictionary
    :returns: Connection object
    """
    c = conn.copy()
    c["connection_id"] = c["conn_id"]
    c.pop("conn_id")
    return Connection(**connection_schema.load(c))


def validate_connections(
    project: Project, environment: str = "default", connection_id: str | None = None
) -> None:
    """
    Validates that the given connections are valid and registers them to Airflow with replace policy for existing
    connections.
    """
    project.load_config(environment=environment)

    # Since we are using the Airflow ORM to interact with connections, we need to tell Airflow to use our airflow.db
    # The usual route is to set $AIRFLOW_HOME before Airflow is imported. However, in the context of the SQL CLI, we
    # decide this during runtime, depending on the project path and SQL CLI configuration.
    airflow_meta_conn = retrieve_airflow_database_conn_from_config(project.directory / project.airflow_home)
    set_airflow_database_conn(airflow_meta_conn)

    config_file_contains_connection = False

    logs = f"\nValidating connection(s) for environment '{environment}'\n"
    for conn in project.connections:
        conn_id = conn["conn_id"]
        conn["connection_id"] = conn_id
        conn.pop("conn_id")
        data = connection_schema.load(conn)
        if connection_id and conn_id != connection_id:
            continue
        if connection_id:
            config_file_contains_connection = True
        conn_obj = Connection(**data)
        _create_or_replace_connection(conn_obj)

        success_status, _ = conn_obj.test_connection()
        if not success_status:
            logs += f"Validating connection {conn_id:{CONNECTION_ID_OUTPUT_STRING_WIDTH}} [bold red]FAILED[/bold red]\n"
            continue

        logs += f"Validating connection {conn_id:{CONNECTION_ID_OUTPUT_STRING_WIDTH}} [bold green]PASSED[/bold green]\n"

    pprint(logs)

    if connection_id and not config_file_contains_connection:
        print("Error: Config file does not contain given connection", connection_id)
