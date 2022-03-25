from astro.settings import SCHEMA
from astro.sql.table import Table


def _handle_table(t: Table):
    """
    returns the fully qualified name ("DATABASE"."SCHEMA"."TABLE").
    This will allow snowflake users to do cross schema and cross database queries
    :param t:
    :return:
    """

    snow_schema = t.schema or SCHEMA
    return t.database + "." + snow_schema + "." + t.table_name


def process_params(parameters):
    return {
        k: (_handle_table(v) if type(v) == Table else v) for k, v in parameters.items()
    }


def add_templates_to_context(parameters, context):
    for k, v in parameters.items():
        if type(v) == Table:
            context[k] = "IDENTIFIER(:" + k + ")"
        else:
            context[k] = ":" + k
    return context
