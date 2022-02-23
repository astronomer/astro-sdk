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
import os
from typing import Optional, Union

from airflow.hooks.sqlite_hook import SqliteHook
from pandas import DataFrame
from pandas.io.sql import SQLDatabase

from astro.utils.dependencies import (
    BigQueryHook,
    PostgresHook,
    SnowflakeHook,
    pandas_tools,
)
from astro.utils.schema_util import create_schema_query, schema_exists


def move_dataframe_to_sql(
    output_table_name,
    conn_id,
    database,
    schema,
    warehouse,
    conn_type,
    df: DataFrame,
    user,
    chunksize,
):
    hook_kwargs = {
        "postgresql": {"postgres_conn_id": conn_id, "schema": database},
        "postgres": {"postgres_conn_id": conn_id, "schema": database},
        "snowflake": {
            "snowflake_conn_id": conn_id,
            "database": database,
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

    if conn_type != "sqlite" and not schema_exists(
        hook=hook, schema=schema, conn_type=conn_type
    ):
        schema_query = create_schema_query(
            conn_type=conn_type, hook=hook, schema_id=schema, user=user
        )
        hook.run(schema_query)

    if conn_type == "snowflake":

        db = SQLDatabase(engine=hook.get_sqlalchemy_engine())
        # make columns uppercase to prevent weird errors in snowflake
        df.columns = df.columns.str.upper()
        db.prep_table(
            df,
            output_table_name.lower(),
            schema=schema,
            if_exists="replace",
            index=False,
        )
        pandas_tools.write_pandas(
            hook.get_conn(),
            df,
            output_table_name,
            chunk_size=chunksize,
            quote_identifiers=False,
        )
    elif conn_type == "bigquery":
        df.to_gbq(
            f"{schema}.{output_table_name}",
            if_exists="replace",
            chunksize=chunksize,
            project_id=hook.project_id,
        )
    elif conn_type == "sqlite":
        df.to_sql(
            output_table_name,
            con=hook.get_sqlalchemy_engine(),
            if_exists="replace",
            chunksize=chunksize,
            method="multi",
            index=False,
        )
    else:
        df.to_sql(
            output_table_name,
            con=hook.get_sqlalchemy_engine(),
            schema=schema,
            if_exists="replace",
            chunksize=chunksize,
            method="multi",
            index=False,
        )
