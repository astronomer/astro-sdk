from __future__ import annotations

import logging
from typing import Any

import yaml
from airflow.api_connexion.exceptions import BadRequest
from airflow.api_connexion.schemas.connection_schema import connection_schema
from airflow.models import Connection
from airflow.utils.session import create_session
from marshmallow.exceptions import ValidationError

from sql_cli.settings import SQL_CLI_PROJECT_DIRECTORY


def _load_yaml_connections(environment: str) -> list[dict[str, Any]]:
    """Gets the configuration yaml for the given environment and loads the connections from it into a dictionary"""
    config_file = SQL_CLI_PROJECT_DIRECTORY / "config" / environment / "configuration.yaml"
    if not config_file.exists():
        raise FileNotFoundError(
            f"Config file configuration.yaml does not exist for environment {environment}"
        )

    with open(config_file) as connections_file:
        connections: list[dict[str, Any]] = yaml.safe_load(connections_file)["connections"]

    return connections


def _test_connection(conn_obj: Connection) -> bool:
    """Tests whether connection is established successfully with the given data."""
    try:
        status, _ = conn_obj.test_connection()
        if not status:
            return False

        return True
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))


def _create_or_replace_connection(conn_obj: Connection) -> str:
    """Creates a new or replaces existing connection in the Airflow DB with the given connection object."""
    conn_id = conn_obj.conn_id
    connection_replaced = False
    with create_session() as session:
        db_connection = session.query(Connection).filter_by(conn_id=conn_id).one_or_none()
        if db_connection:
            session.delete(db_connection)
            session.commit()
            connection_replaced = True
        session.add(conn_obj)
        session.commit()

    if connection_replaced:
        log = f"Validating connection {conn_id:25} PASSED and REPLACED\n"
    else:
        log = f"Validating connection {conn_id:25} PASSED and ADDED\n"
    return log


def validate_connections(environment: str = "default", connection: str | None = None) -> None:
    """
    Validates that the given connections are valid and registers them to Airflow with replace policy for existing
    connections.
    """
    config_file_contains_connection = False
    connections = _load_yaml_connections(environment)
    logs = f"\nValidating connection(s) for environment '{environment}'\n"
    for conn in connections:
        conn_id = conn["conn_id"]
        conn["connection_id"] = conn_id
        conn.pop("conn_id")
        data = connection_schema.load(conn)
        if connection and conn_id != connection:
            continue
        if connection:
            config_file_contains_connection = True
        conn_obj = Connection(**data)

        log = _create_or_replace_connection(conn_obj)

        if not _test_connection(conn_obj):
            logs += f"Validating connection {conn_id:25} FAILED\n"
            continue

        logs += log

    logging.info(logs)

    if connection and not config_file_contains_connection:
        logging.info("Config file does not contain given connection %s", connection)
