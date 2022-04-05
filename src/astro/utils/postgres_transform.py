from astro.sql.table import Table


def add_templates_to_context(parameters, context):
    for k, v in parameters.items():
        if isinstance(v, Table):
            context[k] = v.qualified_name()
        else:
            context[k] = ":" + k
    return context
