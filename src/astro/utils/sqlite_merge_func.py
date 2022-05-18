from astro.sql.table import Table


def sqlite_merge_func(
    target_table: Table,
    merge_table: Table,
    merge_keys,
    target_columns,
    merge_columns,
    conflict_strategy,
):
    statement = "INSERT INTO {main_table} ({target_columns}) SELECT {append_columns} FROM {append_table} Where true"

    if conflict_strategy == "ignore":
        statement += " ON CONFLICT ({merge_keys}) DO NOTHING"
    elif conflict_strategy == "update":
        statement += " ON CONFLICT ({merge_keys}) DO UPDATE SET {update_statements}"

    append_column_names = list(merge_columns)
    target_column_names = list(target_columns)
    column_pairs = list(zip(target_column_names, target_column_names))
    update_statements = [f"{x}=EXCLUDED.{y}" for x, y in column_pairs]

    query = statement.format(
        target_columns=",".join(target_column_names),
        main_table=target_table.name,
        append_columns=",".join(append_column_names),
        append_table=merge_table.name,
        update_statements=",".join(update_statements),
        merge_keys=",".join(list(merge_keys)),
    )
    return query
