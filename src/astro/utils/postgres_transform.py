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
import inspect

from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extensions import AsIs

from astro.sql.table import Table


def parse_template(sql):
    return sql.replace("{", "%(").replace("}", ")s")


def process_params(parameters, python_callable):
    param_types = inspect.signature(python_callable).parameters
    return {
        k: (
            AsIs(v.schema + "." + v.table_name if v.schema else v.table_name)
            if param_types.get(k)
            and param_types.get(k).annotation == Table
            or type(v) == Table
            else v
        )
        for k, v in parameters.items()
    }


def create_sql_engine(postgres_conn_id, database):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema=database)
    engine = hook.get_sqlalchemy_engine()
    engine.url.database = database
    return engine
