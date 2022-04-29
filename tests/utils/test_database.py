from urllib.parse import urlparse

import pytest
from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook
from sqlalchemy import text
from sqlalchemy.engine.base import Engine

from astro.constants import Database
from astro.utils.database import (
    create_database_from_conn_id,
    get_database_name,
    get_sqlalchemy_engine,
    run_sql,
)
from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook


def describe_create_database():
    def with_supported_databases(session):
        assert create_database_from_conn_id("postgres_default") == Database.POSTGRES
        assert create_database_from_conn_id("sqlite_default") == Database.SQLITE
        assert create_database_from_conn_id("google_cloud_default") == Database.BIGQUERY
        assert create_database_from_conn_id("snowflake_conn") == Database.SNOWFLAKE

    def with_unsupported_database(session):
        with pytest.raises(ValueError) as exc_info:
            assert create_database_from_conn_id("cassandra_default")
        expected_msg = "Unsupported database <cassandra>"
        assert exc_info.value.args[0] == expected_msg


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
        engine = hook.get_sqlalchemy_engine()
        db = get_database_name(engine)
        assert db == Database.SQLITE

    def with_unsupported_hook():
        hook = BaseHook()
        with pytest.raises(ValueError) as exc_info:
            get_database_name(hook)
        expected_msg = "Unsupported database <class 'airflow.hooks.base.BaseHook'>"
        assert exc_info.value.args[0] == expected_msg


def describe_get_sqlalchemy_engine():
    def with_sqlite():
        hook = SqliteHook(sqlite_conn_id="sqlite_conn")
        engine = get_sqlalchemy_engine(hook)
        assert isinstance(engine, Engine)
        url = urlparse(str(engine.url))
        assert url.path == "////tmp/sqlite.db"

    def with_sqlite_default_conn():
        hook = SqliteHook()
        engine = get_sqlalchemy_engine(hook)
        assert isinstance(engine, Engine)
        url = urlparse(str(engine.url))
        assert url.path == "//tmp/sqlite_default.db"

    def with_postgres():
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        engine = get_sqlalchemy_engine(hook)
        assert isinstance(engine, Engine)

    def with_snowflake():
        hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        engine = get_sqlalchemy_engine(hook)
        assert isinstance(engine, Engine)

    def with_google_bigquery_hook():
        hook = BigQueryHook()
        engine = get_sqlalchemy_engine(hook)
        assert isinstance(engine, Engine)


def describe_run_sql():
    def with_str():
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        engine = get_sqlalchemy_engine(hook)
        statement = "SELECT nspname FROM pg_catalog.pg_namespace;"
        result = run_sql(engine, statement)
        schemas = [item[0] for item in result]
        assert "public" in schemas

    def with_str_and_parameter():
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        engine = get_sqlalchemy_engine(hook)
        statement = "SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname=:value;"
        result = run_sql(engine, statement, {"value": "public"})
        schemas = result.first()
        assert "public" == schemas[0]

    def with_text():
        hook = PostgresHook(postgres_conn_id="postgres_conn")
        engine = get_sqlalchemy_engine(hook)
        statement = text("SELECT nspname FROM pg_catalog.pg_namespace;")
        result = run_sql(engine, statement)
        schemas = [item[0] for item in result]
        assert "public" in schemas
