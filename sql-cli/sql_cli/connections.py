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
    config_file = (
        SQL_CLI_PROJECT_DIRECTORY / "config" / environment / "configuration.yaml"
    )
    if not config_file.exists():
        raise FileNotFoundError(
            f"Config file configuration.yaml does not exist for environment {environment}"
        )

    connections = yaml.safe_load(open(config_file))["connections"]
    return connections


def _test_connection(conn_obj: Connection, environment: str) -> bool:
    """Tests whether connection is established successfully with the given data."""
    conn_id = conn_obj.conn_id
    try:
        status, message = conn_obj.test_connection()
        if not status:
            logging.info(
                "Connection %s failed in %s environment with error %s",
                conn_id,
                environment,
                message,
            )
            return False
        logging.info(
            "Connection %s tested successfully in %s environment", conn_id, environment
        )
        return True
    except ValidationError as err:
        raise BadRequest(detail=str(err.messages))


def _create_or_replace_connection(conn_obj: Connection, environment: str) -> None:
    """Creates a new or replaces existing connection in the Airflow DB with the given connection object."""
    conn_id = conn_obj.conn_id
    with create_session() as session:
        db_connection = (
            session.query(Connection).filter_by(conn_id=conn_id).one_or_none()
        )
        if db_connection:
            session.delete(db_connection)
            session.commit()
            logging.info(
                "Existing connection %s deleted successfully in %s environment",
                conn_id,
                environment,
            )
        session.add(conn_obj)
        session.commit()
        logging.info(
            "Connection %s added successfully in %s environment", conn_id, environment
        )


def validate_connections(
    environment: str = "default", connection: str | None = None
) -> None:
    """
    Validates that the given connections are valid and registers them to Airflow with replace policy for existing
    connections.
    """
    config_file_contains_connection = False
    connections = _load_yaml_connections(environment)
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

        if not _test_connection(conn_obj, environment):
            continue

        _create_or_replace_connection(conn_obj, environment)

    if connection and not config_file_contains_connection:
        logging.info("Config file does not contain given connection %s", connection)
