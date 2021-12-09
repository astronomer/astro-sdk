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
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql

from astro.sql.table import Table


def postgres_append_func(
    main_table: Table, columns, casted_columns, append_table: Table, conn_id
):
    if columns or casted_columns:
        statement = "INSERT INTO {main_table} ({main_cols}{sep}{main_casted_cols})(SELECT {fields}{sep}{casted_fields} FROM {append_table})"
    else:
        statement = "INSERT INTO {main_table} (SELECT * FROM {append_table})"

    column_names = [sql.Identifier(c) for c in columns]
    casted_column_names = [sql.Identifier(k) for k in casted_columns.keys()]
    fields = [sql.Identifier(*append_table.identifier_args(), c) for c in columns]
    casted_fields = [
        sql.SQL("CAST({k} AS {v})").format(k=sql.Identifier(k), v=sql.SQL(v))
        for k, v in casted_columns.items()
    ]

    query = sql.SQL(statement).format(
        main_cols=sql.SQL(",").join(column_names),
        main_casted_cols=sql.SQL(",").join(casted_column_names),
        main_table=sql.Identifier(*main_table.identifier_args()),
        fields=sql.SQL(",").join(fields),
        sep=sql.SQL(",") if columns and casted_columns else sql.SQL(""),
        casted_fields=sql.SQL(",").join(casted_fields),
        append_table=sql.Identifier(*append_table.identifier_args()),
    )

    hook = PostgresHook(postgres_conn_id=conn_id)
    return query.as_string(hook.get_conn())
