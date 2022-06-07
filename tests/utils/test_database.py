import pytest
import sqlalchemy
from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from astro.constants import Database
from astro.utils.database import get_database_name
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook


def describe_get_database_name():
    def with_google_bigquery_hook():
        hook = BigQueryHook()
        db = get_database_name(hook)
        assert db == Database.BIGQUERY
        engine = hook.get_sqlalchemy_engine()
        db = get_database_name(engine)
        assert db == Database.BIGQUERY

    def with_snowflake_hook():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        db = get_database_name(hook)
        assert db == Database.SNOWFLAKE
        engine = hook.get_sqlalchemy_engine()
        db = get_database_name(engine)
        assert db == Database.SNOWFLAKE

    def with_postgres_hook():
        hook = PostgresHook()
        db = get_database_name(hook)
        assert db == Database.POSTGRES
        engine = hook.get_sqlalchemy_engine()
        db = get_database_name(engine)
        assert db == Database.POSTGRES

    def with_sqlite_hook():
        hook = SqliteHook()
        db = get_database_name(hook)
        assert db == Database.SQLITE
        engine = create_sqlalchemy_engine_with_sqlite(hook)
        db = get_database_name(engine)
        assert db == Database.SQLITE

    def with_unsupported_hook():
        hook = BaseHook()
        with pytest.raises(ValueError) as exc_info:
            get_database_name(hook)
        expected_msg = "Unsupported database <class 'airflow.hooks.base.BaseHook'>"
        assert exc_info.value.args[0] == expected_msg


def create_sqlalchemy_engine_with_sqlite(hook: SqliteHook) -> sqlalchemy.engine.Engine:
    # Airflow uses sqlite3 library and not SqlAlchemy for SqliteHook
    # and it only uses the hostname directly.
    airflow_conn = hook.get_connection(getattr(hook, hook.conn_name_attr))
    engine = sqlalchemy.create_engine(f"sqlite:///{airflow_conn.host}")
    return engine
