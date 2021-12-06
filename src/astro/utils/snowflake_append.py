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
import re


def _wrap_identifiers(sql, identifier_params):
    all_vals = re.findall("%\(.*?\)s", sql)
    mod_vals = {
        f: f"IDENTIFIER({f})" if f[2:-2] in identifier_params else f for f in all_vals
    }
    for k, v in mod_vals.items():
        sql = sql.replace(k, v)
    return sql


def snowflake_append_func(
    main_table, columns, casted_columns, append_table, snowflake_conn_id
):
    def wrap_identifier(inp):
        return "Identifier(%(" + inp + ")s)"

    if columns or casted_columns:
        statement = (
            "INSERT INTO Identifier(%(main_table)s) ({main_cols}{sep}{main_casted_cols})"
            "(SELECT {fields}{sep}{casted_fields} FROM Identifier(%(append_table)s))"
        )
    else:
        statement = "INSERT INTO Identifier(%(main_table)s) (SELECT * FROM Identifier(%(append_table)s))"

    col_dict = {
        f"col{i}": x for i, x in enumerate(columns)
    }  # prevent directly putting user input into query

    casted_col_dict = {f"casted_col{i}": x for i, x in enumerate(casted_columns.keys())}

    statement = (
        statement.replace("{sep}", ",")
        if columns and casted_columns
        else statement.replace("{sep}", "")
    )
    statement = statement.replace(
        "{main_cols}", ",".join([c for c in columns])
    )  # Please note that we are not wrapping these in Identifier due to a snowflake bug. Must fix before public release!
    statement = statement.replace(
        "{main_casted_cols}", ",".join([c for c in casted_columns.keys()])
    )  # Please note that we are not wrapping these in Identifier due to a snowflake bug. Must fix before public release!
    statement = statement.replace(
        "{fields}", ",".join([wrap_identifier(c) for c in col_dict.keys()])
    )
    statement = statement.replace(
        "{casted_fields}",
        ",".join(
            [
                "CAST(" + wrap_identifier(c) + " AS " + casted_columns[v] + ")"
                for c, v in casted_col_dict.items()
            ]
        ),
    )
    statement = statement.replace("{", "%(").replace("}", ")s")
    statement = _wrap_identifiers(
        sql=statement,
        identifier_params=[main_table, columns, casted_columns.keys(), append_table],
    )
    col_dict.update(casted_col_dict)
    col_dict["main_table"] = main_table
    col_dict["append_table"] = append_table
    return statement, col_dict
