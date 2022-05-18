from astro.databases import create_database
from astro.sql.table import Table


def bigquery_merge_func(
    target_table: Table,
    merge_table: Table,
    merge_keys,
    target_columns,
    merge_columns,
    conflict_strategy,
):
    database = create_database(target_table.conn_id)
    target_table_name = database.get_table_qualified_name(target_table)
    merge_table_name = database.get_table_qualified_name(merge_table)

    statement = f"MERGE {target_table_name} T USING {merge_table_name} S\
                ON {' AND '.join(['T.' + col + '= S.' + col for col in merge_keys])}\
                WHEN NOT MATCHED BY TARGET THEN INSERT ({','.join(target_columns)}) VALUES ({','.join(merge_columns)})"

    if conflict_strategy == "update":
        update_statement = "UPDATE SET {}".format(
            ", ".join(
                [
                    f"T.{target_columns[idx]}=S.{merge_columns[idx]}"
                    for idx in range(len(target_columns))
                ]
            )
        )
        statement += f" WHEN MATCHED THEN {update_statement}"
    return statement, {}
