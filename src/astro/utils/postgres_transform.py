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
