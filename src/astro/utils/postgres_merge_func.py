from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql


def postgres_merge_func(
    target_table,
    merge_table,
    merge_keys,
    target_columns,
    merge_columns,
    conn_id,
    conflict_strategy,
):
    statement = "INSERT INTO {main_table} ({target_columns}) SELECT {append_columns} FROM {append_table}"

    if conflict_strategy == "ignore":
        statement += " ON CONFLICT ({merge_keys}) DO NOTHING"
    elif conflict_strategy == "update":
        statement += " ON CONFLICT ({merge_keys}) DO UPDATE SET {update_statements}"

    append_column_names = [sql.Identifier(c) for c in merge_columns]
    target_column_names = [sql.Identifier(c) for c in target_columns]
    column_pairs = list(zip(target_column_names, target_column_names))
    update_statements = [
        sql.SQL("{x}=EXCLUDED.{y}").format(x=x, y=y) for x, y in column_pairs
    ]

    query = sql.SQL(statement).format(
        target_columns=sql.SQL(",").join(target_column_names),
        main_table=sql.Identifier(target_table),
        append_columns=sql.SQL(",").join(append_column_names),
        append_table=sql.Identifier(merge_table),
        update_statements=sql.SQL(",").join(update_statements),
        merge_keys=sql.SQL(",").join([sql.Identifier(x) for x in merge_keys]),
    )

    hook = PostgresHook(postgres_conn_id=conn_id)
    return query.as_string(hook.get_conn())
