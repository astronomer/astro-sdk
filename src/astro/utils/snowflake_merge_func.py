from airflow.exceptions import AirflowException

from astro.sql.table import Table


def wrap_identifier(inp):
    return "Identifier({{" + inp + "}})"


def snowflake_merge_func(
    target_table: Table,
    merge_table: Table,
    target_columns,
    merge_keys,
    merge_columns,
    conflict_strategy,
):
    statement = "merge into {{main_table}} using {{merge_table}} on " "{merge_clauses}"

    merge_target_dict = {
        "merge_clause_target_" + str(i): target_table.name + "." + x
        for i, x in enumerate(merge_keys.keys())
    }
    merge_append_dict = {
        "merge_clause_append_" + str(i): merge_table.name + "." + x
        for i, x in enumerate(merge_keys.values())
    }

    statement = fill_in_merge_clauses(merge_append_dict, merge_target_dict, statement)

    values_to_check = [target_table.name, merge_table.name]
    values_to_check.extend(target_columns)
    values_to_check.extend(merge_columns)
    for v in values_to_check:
        if not is_valid_snow_identifier(v):
            raise AirflowException(
                f"The identifier {v} is invalid. Please check to prevent SQL injection"
            )

    if conflict_strategy == "update":
        statement += " when matched then UPDATE SET {merge_vals}"
        statement = fill_in_update_statement(
            statement,
            target_table.name,
            merge_table.name,
            target_columns,
            merge_columns,
        )
    statement += (
        " when not matched then insert({target_columns}) values ({append_columns})"
    )
    statement = fill_in_append_statements(
        target_table.name,
        merge_table.name,
        statement,
        target_columns,
        merge_columns,
    )

    params = {}
    params.update(merge_target_dict)
    params.update(merge_append_dict)
    params["main_table"] = target_table
    params["merge_table"] = merge_table

    return statement, params


def fill_in_append_statements(
    main_table,
    merge_table,
    statement,
    targest_columns,
    merge_columns,
):
    statement = statement.replace(
        "{target_columns}",
        ",".join(f"{main_table}.{t}" for t in targest_columns),
    )
    statement = statement.replace(
        "{append_columns}",
        ",".join(f"{merge_table}.{t}" for t in merge_columns),
    )
    return statement


def fill_in_update_statement(
    statement, main_table, merge_table, target_columns, merge_columns
):
    merge_statement = ",".join(
        [
            f"{main_table}.{t}={merge_table}.{m}"
            for t, m in zip(target_columns, merge_columns)
        ]
    )
    statement = statement.replace("{merge_vals}", merge_statement)
    return statement


def fill_in_merge_clauses(merge_append_dict, merge_target_dict, statement):
    return statement.replace(
        "{merge_clauses}",
        " AND ".join(
            wrap_identifier(k) + "=" + wrap_identifier(v)
            for k, v in zip(merge_target_dict.keys(), merge_append_dict.keys())
        ),
    )


def is_valid_snow_identifier(name):
    """
    Because Snowflake does not allow using `Identifier` for inserts or updates, we need to make reasonable attempts to
    ensure that no one can perform a SQL injection using this method. The following method ensures that a string
    follows the expected identifier syntax https://docs.snowflake.com/en/sql-reference/identifiers-syntax.html
    @param name:
    @return:
    """
    if not 1 <= len(name) <= 255:
        return False

    name_is_quoted = name[0] == '"'
    if name_is_quoted:
        if len(name) < 2 or name[-1] != '"':
            return False  # invalid because no closing quote

        return ensure_internal_quotes_closed(name)
    return ensure_only_valid_characters(name)


# test code to check for validate snowflake identifier
def ensure_internal_quotes_closed(name):
    last_quoted = False
    for c in name[1:-1]:
        if last_quoted:
            if c != '"':
                return False
            last_quoted = False
        elif c == '"':
            last_quoted = True
        # any character is fair game inside a properly quoted name

    if last_quoted:
        return False  # last quote was not escape

    return True


def ensure_only_valid_characters(name):
    if not (name[0].isalpha()) and name[0] != "_":
        return False
    for c in name[1:]:
        if not (c.isalpha() or c.isdigit() or c == "_" or c == "$"):
            return False
    return True
