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

from astro.sql.table import Table
from astro.utils.dependencies import PostgresHook


def add_templates_to_context(parameters, context):
    for k, v in parameters.items():
        if type(v) == Table:
            final_name = v.schema + "." + v.table_name if v.schema else v.table_name
            context[k] = final_name
        else:
            context[k] = ":" + k
    return context


def create_sql_engine(postgres_conn_id, database):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema=database)
    engine = hook.get_sqlalchemy_engine()
    engine.url.database = database
    return engine
