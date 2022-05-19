from astro.databases import create_database
from astro.sql.table import Table


def add_templates_to_context(parameters, context):
    for k, v in parameters.items():
        if isinstance(v, Table):
            database = create_database(v.conn_id)
            context[k] = database.get_table_qualified_name(v)
        else:
            context[k] = ":" + k
    return context
