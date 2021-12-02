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
            AsIs(v.table_name)
            if param_types.get(k) and param_types.get(k).annotation == Table
            else v
        )
        for k, v in parameters.items()
    }


def create_sql_engine(postgres_conn_id, database):
    hook = PostgresHook(postgres_conn_id=postgres_conn_id, schema=database)
    engine = hook.get_sqlalchemy_engine()
    engine.url.database = database
    return engine
