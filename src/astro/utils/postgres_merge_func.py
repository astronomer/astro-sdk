from astro.sql.table import Table
from astro.utils.dependencies import PostgresHook, postgres_sql


def postgres_merge_func(
    target_table: Table,
    merge_table: Table,
    merge_keys,
    target_columns,
    merge_columns,
    conflict_strategy,
):
    statement = "INSERT INTO {main_table} ({target_columns}) SELECT {append_columns} FROM {append_table}"

    if conflict_strategy == "ignore":
        statement += " ON CONFLICT ({merge_keys}) DO NOTHING"
    elif conflict_strategy == "update":
        statement += " ON CONFLICT ({merge_keys}) DO UPDATE SET {update_statements}"

    append_column_names = [postgres_sql.Identifier(c) for c in merge_columns]
    target_column_names = [postgres_sql.Identifier(c) for c in target_columns]
    column_pairs = list(zip(target_column_names, target_column_names))
    update_statements = [
        postgres_sql.SQL("{x}=EXCLUDED.{y}").format(x=x, y=y) for x, y in column_pairs
    ]

    query = postgres_sql.SQL(statement).format(
        target_columns=postgres_sql.SQL(",").join(target_column_names),
        main_table=postgres_sql.Identifier(*identifier_args(target_table)),
        append_columns=postgres_sql.SQL(",").join(append_column_names),
        append_table=postgres_sql.Identifier(*identifier_args(merge_table)),
        update_statements=postgres_sql.SQL(",").join(update_statements),
        merge_keys=postgres_sql.SQL(",").join(
            [postgres_sql.Identifier(x) for x in merge_keys]
        ),
    )

    hook = PostgresHook(postgres_conn_id=target_table.conn_id)
    return query.as_string(hook.get_conn())


def identifier_args(table):
    schema = table.metadata.schema
    return (schema, table.name) if schema else (table.name,)
