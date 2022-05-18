from astro.settings import SCHEMA
from astro.sql.table import Table


def _handle_table(t: Table):
    """
    returns the fully qualified name ("DATABASE"."SCHEMA"."TABLE").
    This will allow snowflake users to do cross schema and cross database queries
    :param t:
    :return:
    """
    schema = getattr(t.metadata, "schema", "") or SCHEMA
    table_name = [getattr(t.metadata, "database", ""), schema, t.name]
    table_name = [x for x in table_name if x]
    return ".".join(table_name)


def process_params(parameters):
    return {
        k: (_handle_table(v) if type(v) is Table else v) for k, v in parameters.items()
    }


def add_templates_to_context(parameters, context):
    for k, v in parameters.items():
        if type(v) is Table:
            context[k] = "IDENTIFIER(:" + k + ")"
        else:
            context[k] = ":" + k
    return context
