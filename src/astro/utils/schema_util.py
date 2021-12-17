import os

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql


def set_schema_query(conn_type, hook, schema_id, user):

    if conn_type == "postgres":
        return (
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {user}")
            .format(schema=sql.Identifier(schema_id), user=sql.Identifier(user))
            .as_string(hook.get_conn())
        )
    elif conn_type == "snowflake":
        return f"CREATE SCHEMA IF NOT EXISTS {schema_id}"


def get_schema():
    return os.getenv("AIRFLOW__ASTRO__SQL_SCHEMA") or "tmp_astro"
