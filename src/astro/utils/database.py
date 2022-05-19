from typing import Union

from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import text
from sqlalchemy.engine import Engine, ResultProxy

from astro.constants import CONN_TYPE_TO_DATABASE, Database
from astro.sqlite_utils import create_sqlalchemy_engine_with_sqlite
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook


def create_database_from_conn_id(conn_id: str) -> Database:
    """
    Given a conn_id, identify the database name.

    :param conn_id: Airflow connection ID
    :type conn_id: str
    :return: the database this interface relates to (e.g. Database.SQLITE)
    :rtype: astro.constants.Database enum item
    """
    conn_type = BaseHook.get_connection(conn_id).conn_type
    try:
        database_name = CONN_TYPE_TO_DATABASE[conn_type]
    except KeyError:
        raise ValueError(f"Unsupported database <{conn_type}>")
    return database_name


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


def get_sqlalchemy_engine(hook: Union[BaseHook, SqliteHook]) -> Engine:
    """
    Given a hook, return a SQLAlchemy engine for the target database.

    :param hook: Airflow Hook used to access a SQL-like database
    :type hook: (BigQueryHook, PostgresHook, SnowflakeHook, SqliteHook)
    :return: SQLAlchemy engine
    :rtype: sqlalchemy.Engine
    """
    database = get_database_name(hook)
    engine = None
    if database == Database.SQLITE:
        engine = create_sqlalchemy_engine_with_sqlite(hook)
    if engine is None:
        engine = hook.get_sqlalchemy_engine()
    return engine


def run_sql(
    engine: Engine,
    sql_statement: Union[str, text],
    parameters: Union[None, dict] = None,
) -> ResultProxy:
    """
    Run a SQL statement using the given engine.

    :param engine: SQLAlchemy engine
    :type engine: sqlalchemy.Engine
    :param sql_statement: SQL statement to be run on the engine
    :type sql_statement: (sqlalchemy.text or str)
    :param parameters: (optional) Parameters to be passed to the SQL statement
    :type parameters: dict
    :return: Result of running the statement
    :rtype: sqlalchemy.engine.ResultProxy
    """
    if parameters is None:
        parameters = {}
    connection = engine.connect()
    if isinstance(sql_statement, str):
        return connection.execute(text(sql_statement), parameters)

    return connection.execute(sql_statement, parameters)
