import os

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from astro.sql.table import Table


def set_schema_query(conn_type, hook, schema_id, user):

    if conn_type in ["postgresql", "postgres"]:
        return (
            sql.SQL("CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {user}")
            .format(schema=sql.Identifier(schema_id), user=sql.Identifier(user))
            .as_string(hook.get_conn())
        )
    elif conn_type in ["snowflake", "google_cloud_platform", "bigquery"]:
        return f"CREATE SCHEMA IF NOT EXISTS {schema_id}"


def get_schema():
    return os.getenv("AIRFLOW__ASTRO__SQL_SCHEMA") or "tmp_astro"


def get_table_name(table: Table):
    conn_type = BaseHook.get_connection(table.conn_id).conn_type
    if conn_type in ["bigquery"]:
        return table.qualified_name()
    return table.table_name
