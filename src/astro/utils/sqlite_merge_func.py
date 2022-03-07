"""
Copyright Astronomer, Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

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

    append_column_names = [c for c in merge_columns]
    target_column_names = [c for c in target_columns]
    column_pairs = list(zip(target_column_names, target_column_names))
    update_statements = [f"{x}=EXCLUDED.{y}" for x, y in column_pairs]

    query = statement.format(
        target_columns=",".join(target_column_names),
        main_table=target_table.table_name,
        append_columns=",".join(append_column_names),
        append_table=merge_table.table_name,
        update_statements=",".join(update_statements),
        merge_keys=",".join([x for x in merge_keys]),
    )
    return query
