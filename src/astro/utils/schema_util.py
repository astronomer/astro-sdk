from typing import List

from astro.sql.table import Table
from astro.utils.dependencies import postgres_sql


def schema_exists(hook, schema, conn_type):
    if conn_type in ["postgresql", "postgres"]:
        created_schemas = hook.run(
            "SELECT schema_name FROM information_schema.schemata;",
            handler=lambda x: [y[0] for y in x.fetchall()],
        )
        return schema.upper() in [c.upper() for c in created_schemas]

    if conn_type == "snowflake":
        created_schemas = [
            x["SCHEMA_NAME"]
            for x in hook.run("SELECT SCHEMA_NAME from information_schema.schemata;")
        ]
        return schema.upper() in [c.upper() for c in created_schemas]
    return False


def create_schema_query(conn_type, hook, schema_id, user):
    if conn_type in ["postgresql", "postgres"]:
        return (
            postgres_sql.SQL(
                "CREATE SCHEMA IF NOT EXISTS {schema} AUTHORIZATION {user}"
            )
            .format(
                schema=postgres_sql.Identifier(schema_id),
                user=postgres_sql.Identifier(user),
            )
            .as_string(hook.get_conn())
        )

    if conn_type in ["snowflake", "google_cloud_platform", "bigquery", "sqlite"]:
        return f"CREATE SCHEMA IF NOT EXISTS {schema_id}"


def tables_from_same_db(tables: List[Table]):
    """
    Validate that the tables belong to same db by checking connection type.
    :param tables: List of table
    :return: Boolean
    """
    conn_ids = set()
    for table in tables:
        if table.conn_id:
            conn_ids.add(table.conn_id)
    return len(conn_ids) == 1


def get_error_string_for_multiple_dbs(tables: List[Table]):
    """
    Get error string for tables belonging to multiple databases.
    :param tables: list of table
    :return: String: error string
    """
    return f'Tables should belong to same db {", ".join([f"{table.name}: {table.conn_id}" for table in tables])}'
