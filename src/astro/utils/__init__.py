from typing import Optional, Type, Union

from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from astro.utils.dependencies import (
    BigQueryHook,
    MissingPackage,
    PostgresHook,
    SnowflakeHook,
)


def get_hook(
    conn_id: str,
    database: Optional[str],
    schema: Optional[str] = None,
) -> Union[BigQueryHook, PostgresHook, SqliteHook, SnowflakeHook, Type[MissingPackage]]:
    """
    Retrieve the relevant Airflow hook depending on the given arguments.
    """
    conn_type = BaseHook.get_connection(conn_id).conn_type
    hook_kwargs = {
        "postgresql": {"postgres_conn_id": conn_id},
        "postgres": {"postgres_conn_id": conn_id},
        "snowflake": {
            "snowflake_conn_id": conn_id,
            "database": database,
            "schema": schema,
        },
        "google_cloud_platform": {"use_legacy_sql": False, "gcp_conn_id": conn_id},
        "bigquery": {"use_legacy_sql": False, "gcp_conn_id": conn_id},
        "gcpbigquery": {"use_legacy_sql": False, "gcp_conn_id": conn_id},
        "sqlite": {"sqlite_conn_id": conn_id},
    }
    type_to_hook = {
        "postgresql": PostgresHook,
        "postgres": PostgresHook,
        "snowflake": SnowflakeHook,
        "google_cloud_platform": BigQueryHook,
        "bigquery": BigQueryHook,
        "gcpbigquery": BigQueryHook,
        "sqlite": SqliteHook,
    }
    try:
        hook_class = type_to_hook[conn_type]
    except KeyError:
        supported_types = list(type_to_hook.keys())
        raise ValueError(
            f"The conn_id {conn_id} is of unsupported type {conn_type}. Current supported types: {supported_types}"
        )
    else:
        hook = hook_class(**hook_kwargs[conn_type])
        if database:
            hook.database = database
        return hook
