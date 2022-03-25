import re


def _wrap_identifiers(sql, identifier_params):
    all_vals = re.findall(r"%\(.*?\)s", sql)
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
            "INSERT INTO %(main_table)s ({main_cols}{sep}{main_casted_cols})"
            "(SELECT {fields}{sep}{casted_fields} FROM %(append_table)s)"
        )
    else:
        statement = "INSERT INTO %(main_table)s (SELECT * FROM %(append_table)s)"

    col_dict = {
        f"col{i}": x for i, x in enumerate(columns)
    }  # prevent directly putting user input into query

    casted_col_dict = {f"casted_col{i}": x for i, x in enumerate(casted_columns.keys())}

    statement = (
        statement.replace("{sep}", ",")
        if columns and casted_columns
        else statement.replace("{sep}", "")
    )
    statement = statement.replace("{main_cols}", ",".join(list(columns)))
    # TODO: Please note that we are not wrapping these in Identifier due to a snowflake bug.
    #  Must fix before public release!
    statement = statement.replace(
        "{main_casted_cols}", ",".join(list(casted_columns.keys()))
    )
    # TODO: Please note that we are not wrapping these in Identifier due to a snowflake bug.
    #  Must fix before public release!
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
