"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""
from airflow.hooks.base import BaseHook
from airflow.providers.sqlite.hooks.sqlite import SqliteHook

from astro.utils.dependencies import BigQueryHook, PostgresHook, SnowflakeHook


def get_hook(conn_id, database, role=None, schema=None, warehouse=None):
    """
    Retrieve the relevant Airflow hook depending on the given arguments.
    """
    conn_type = BaseHook.get_connection(conn_id).conn_type
    hook_kwargs = {
        "postgresql": {"postgres_conn_id": conn_id, "schema": database},
        "postgres": {"postgres_conn_id": conn_id, "schema": database},
        "snowflake": {
            "snowflake_conn_id": conn_id,
            "database": database,
            "role": role,
            "schema": schema,
            "warehouse": warehouse,
        },
        "bigquery": {"use_legacy_sql": False, "gcp_conn_id": conn_id},
        "sqlite": {"sqlite_conn_id": conn_id},
    }
    try:
        hook_class = {
            "postgresql": PostgresHook,
            "postgres": PostgresHook,
            "snowflake": SnowflakeHook,
            "bigquery": BigQueryHook,
            "sqlite": SqliteHook,
        }[conn_type]
    except KeyError:
        raise ValueError(
            f"The conn_id {conn_id} is of unsupported type {conn_type}. Current supported types: {list(hook_class.keys())}"
        )
    else:
        hook = hook_class(**hook_kwargs[conn_type])
        if database:
            hook.database = database
        return hook
