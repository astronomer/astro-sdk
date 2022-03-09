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


def bigquery_merge_func(
    target_table: Table,
    merge_table: Table,
    merge_keys,
    target_columns,
    merge_columns,
    conflict_strategy,
):
    statement = f"MERGE {target_table.schema}.{target_table.table_name} T USING {merge_table.schema}.{merge_table.table_name} S\
                ON {' AND '.join(['T.'+col +'= S.'+col for col in merge_keys])}\
                WHEN NOT MATCHED BY TARGET THEN INSERT ({','.join(target_columns)}) VALUES ({','.join(merge_columns)})"

    if conflict_strategy == "update":
        update_statement = f"UPDATE SET {', '.join(['T.' + target_columns[index] + '=S.' + merge_columns[index] for index in range(len(target_columns))])}"
        statement += f" WHEN MATCHED THEN {update_statement}"

    return statement, {}
