from typing import Union

from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy.engine import Engine

from astro.constants import Database
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook


def get_database_name(interface: Union[Engine, BaseHook, SqliteHook]) -> Database:
    """
    Given a hook or a SQL engine, identify the database name.

    :param interface: interface to the database
    :type interface: SQLAlchemy engine or Airflow Hook (BigQueryHook, PostgresHook, SnowflakeHook, SqliteHook)
    :return: the database this interface relates to (e.g. Database.SQLITE)
    :rtype: astro.constants.Database enum item
    """
    if isinstance(interface, BaseHook):
        hook_to_database = {
            BigQueryHook: Database.BIGQUERY,
            PostgresHook: Database.POSTGRES,
            SnowflakeHook: Database.SNOWFLAKE,
            SqliteHook: Database.SQLITE,
        }
        try:
            database_name = hook_to_database[type(interface)]
        except KeyError:
            raise ValueError(f"Unsupported database {type(interface)}")
    else:  # SqlAlchemy engine
        database_name = getattr(Database, interface.name.upper())
    return database_name
