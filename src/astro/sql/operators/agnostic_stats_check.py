from distutils import log as logger
from os import name, stat
from typing import Dict, List, Set

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from sqlalchemy.sql.expression import table
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.schema import Table

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator
from astro.sql.table import Table
from astro.utils.snowflake_merge_func import (
    is_valid_snow_identifier,
    is_valid_snow_identifiers,
)


class OutlierCheck:
    def __init__(
        self,
        name: str,
        columns_map: map,
        accepted_std_div: int,
        threshold: float,
    ) -> None:
        self.name = name
        self.columns_map = columns_map
        self.accepted_std_div = accepted_std_div
        self.threshold = threshold


class ChecksHandler:
    def __init__(self, checks: List[OutlierCheck], conn_type: str):
        self.checks = checks
        self.conn_type = conn_type

    def prepare_main_stats_postgres_sql(self, main_table: Table):
        select_expressions = []
        for check in self.checks:
            for key in check.columns_map:
                select_expressions.append(
                    sql.SQL(
                        "stddev({key}) as {check_name}_{key_name}_stddev, avg({key}) as {check_name}_{key_name}_avg"
                    ).format(
                        key=sql.Identifier(key),
                        key_name=sql.SQL(key),
                        check_name=sql.SQL(check.name),
                    )
                )

        return sql.SQL(
            "(SELECT {select_expressions} FROM {main_table}) as main_stats"
        ).format(
            select_expressions=sql.SQL(" ,").join(select_expressions),
            main_table=sql.Identifier(main_table.table_name),
        )

    def prepare_main_stats_snowflake_sql(self, main_table: Table):
        select_expressions = []
        for check in self.checks:
            for key in check.columns_map:
                stats_query = "stddev({key}) as {check_name}_{key}_stddev, avg({key}) as {check_name}_{key}_avg"
                replacements = {"{key}": key, "{check_name}": check.name}

                invalid_identifier = is_valid_snow_identifiers(
                    [key, check.name, check.name]
                )
                if len(invalid_identifier) > 0:
                    raise ValueError(
                        f"Not a valid snowflake identifier {', '.join(invalid_identifier)}"
                    )
                for key, val in replacements.items():
                    stats_query = stats_query.replace(key, val)
                select_expressions.append(stats_query)

        if not is_valid_snow_identifier(main_table.table_name):
            raise ValueError(
                f"Not a valid snowflake identifier {main_table.table_name}"
            )
        statement = "(SELECT {select_expressions} FROM Identifier('{main_table}')) as main_stats"
        replacements = {
            "{main_table}": main_table.table_name,
            "{select_expressions}": " ,".join(select_expressions),
        }
        for key, val in replacements.items():
            statement = statement.replace(key, val)
        return statement

    def prepare_column_postgres_sql(self, check):
        column_sql = []
        for column in check.columns_map:
            main_table_col = column
            compare_table_col = check.columns_map[column]
            statement = sql.SQL(
                """({compare_table_col} > (main_stats.{check_name}_{main_table_col}_avg + (main_stats.{check_name}_{main_table_col}_stddev * {accepted_std_div})))
                OR
                ({compare_table_col} < (main_stats.{check_name}_{main_table_col}_avg - (main_stats.{check_name}_{main_table_col}_stddev * {accepted_std_div})))
                """
            ).format(
                compare_table_col=sql.Identifier(compare_table_col),
                check_name=sql.SQL(check.name),
                main_table_col=sql.SQL(main_table_col),
                accepted_std_div=sql.SQL(str(check.accepted_std_div)),
            )
            column_sql.append(statement)

        return sql.SQL("OR").join(column_sql)

    def prepare_column_snowflake_sql(self, check):
        column_sql = []
        for column in check.columns_map:
            main_table_col = column
            compare_table_col = check.columns_map[column]
            replacements = {
                "{compare_table_col}": compare_table_col,
                "{check_name}": check.name,
                "{main_table_col}": main_table_col,
                "{accepted_std_div}": str(check.accepted_std_div),
            }

            invalid_identifier = is_valid_snow_identifiers(
                [compare_table_col, check.name, main_table_col]
            )
            if len(invalid_identifier) > 0:
                raise ValueError(
                    f"Not a valid snowflake identifier {', '.join(invalid_identifier)}"
                )

            statement = """(Identifier('{compare_table_col}') > (Identifier('main_stats.{check_name}_{main_table_col}_avg') + (Identifier('main_stats.{check_name}_{main_table_col}_stddev') * {accepted_std_div})))
                OR
                (Identifier('{compare_table_col}') < (Identifier('main_stats.{check_name}_{main_table_col}_avg') - (Identifier('main_stats.{check_name}_{main_table_col}_stddev') * {accepted_std_div})))
                """
            for key, val in replacements.items():
                statement = statement.replace(key, val)
            column_sql.append(statement)

        return "OR".join(column_sql)

    def prepare_cases_postgres_sql(self):
        cases = []
        for check in self.checks:
            statement = sql.SQL(
                "CASE WHEN {column_sql} THEN 1 ELSE 0 END as {check_name}"
            ).format(
                column_sql=self.prepare_column_postgres_sql(check),
                check_name=sql.SQL(check.name),
            )
            cases.append(statement)
        return sql.SQL(",").join(cases)

    def prepare_cases_snowflake_sql(self):
        cases = []
        for check in self.checks:
            statement = "CASE WHEN {column_sql} THEN 1 ELSE 0 END as {check_name}"
            replacements = {
                "{column_sql}": self.prepare_column_snowflake_sql(check),
                "{check_name}": check.name,
            }
            for key, val in replacements.items():
                statement = statement.replace(key, val)
            cases.append(statement)
        return ",".join(cases)

    def prepare_checks_postgres_aggregation(self):
        aggregations = []
        # sum(room_check) as room_check,
        for check in self.checks:
            aggregations.append(
                sql.SQL("sum({name}) as {name}").format(name=sql.Identifier(check.name))
            )
        return sql.SQL(", ").join(aggregations)

    def prepare_checks_snowflake_aggregation(self):
        aggregations = []
        # sum(room_check) as room_check,
        for check in self.checks:
            statement = "sum({name}) as {name}"
            replacements = {"{name}": check.name}
            for key, val in replacements.items():
                statement = statement.replace(key, val)

            aggregations.append(statement)
        return ", ".join(aggregations)

    def prepare_comparison_sql(self, main_table: Table, compare_table: Table):
        statement = ""
        if self.conn_type == "postgres":
            hook = PostgresHook(postgres_conn_id=main_table.conn_id)
            statement = (
                sql.SQL(
                    "SELECT count(*) as total, {checks} FROM (SELECT {cases}, {compare_table}.*  FROM {main_stats}, {compare_table}) as results"
                )
                .format(
                    cases=self.prepare_cases_postgres_sql(),
                    main_stats=self.prepare_main_stats_postgres_sql(main_table),
                    compare_table=sql.Identifier(compare_table.table_name),
                    checks=self.prepare_checks_postgres_aggregation(),
                )
                .as_string(hook.get_conn())
            )
        elif self.conn_type == "snowflake":
            statement = "SELECT count(*) as total, {checks} FROM (SELECT {cases}, {compare_table}.*  FROM {main_stats}, Identifier('{compare_table}')) as results"
            replacements = {
                "{cases}": self.prepare_cases_snowflake_sql(),
                "{compare_table}": compare_table.table_name,
                "{main_stats}": self.prepare_main_stats_snowflake_sql(main_table),
                "{checks}": self.prepare_checks_snowflake_aggregation(),
            }

            invalid_identifier = is_valid_snow_identifiers([compare_table.table_name])
            if len(invalid_identifier) > 0:
                raise ValueError(
                    f"Not a valid snowflake identifier {', '.join(invalid_identifier)}"
                )

            for key, val in replacements.items():
                statement = statement.replace(key, val)
        else:
            raise ValueError("Unsupported connection type")
        return statement, {}

    def check_snowflake_results(self, row, check, index):
        return row[check.name.upper()]

    def check_postgres_results(self, row, check, index):
        return row[index + 1]

    def evaluate_results(self, rows, max_rows_returned, conn_type):
        check_results = {
            "postgres": self.check_postgres_results,
            "snowflake": self.check_snowflake_results,
        }[conn_type]

        total_rows = 0
        if conn_type == "snowflake":
            total_rows = rows[0]["TOTAL"]
        elif conn_type == "postgres":
            total_rows = rows[0][0]

        failed_rows_count = {
            check.name: check_results(rows[0], check, index)
            for index, check in enumerate(self.checks)
        }
        failed_checks = set()
        for check in self.checks:
            if failed_rows_count[check.name] / total_rows > check.threshold:
                failed_checks.add(check.name)
        return failed_checks

    def prepare_failed_checks_results(
        self,
        conn_type,
        main_table: Table,
        compare_table: Table,
        failed_checks: Set[str],
        max_rows_returned,
    ):
        failed_checks_sql: List[tuple] = []
        for check in self.checks:
            if check.name in failed_checks:
                if conn_type == "postgres":
                    hook = PostgresHook(postgres_conn_id=main_table.conn_id)
                    statement = (
                        sql.SQL(
                            "SELECT {compare_table}.* FROM {main_stats}, {compare_table} WHERE {column_sql} limit {max_rows_returned}"
                        )
                        .format(
                            compare_table=sql.Identifier(compare_table.table_name),
                            main_stats=self.prepare_main_stats_postgres_sql(main_table),
                            column_sql=self.prepare_column_postgres_sql(check),
                            max_rows_returned=sql.SQL(str(max_rows_returned)),
                        )
                        .as_string(hook.get_conn())
                    )
                    failed_checks_sql.append((check.name, statement, {}))
                elif conn_type == "snowflake":
                    statement = "SELECT {compare_table}.* FROM {main_stats}, Identifier('{compare_table}') WHERE {column_sql} limit {max_rows_returned}"
                    replacements = {
                        "{compare_table}": compare_table.table_name,
                        "{main_stats}": self.prepare_main_stats_snowflake_sql(
                            main_table
                        ),
                        "{column_sql}": self.prepare_column_snowflake_sql(check),
                        "{max_rows_returned}": str(max_rows_returned),
                    }

                    invalid_identifier = is_valid_snow_identifiers(
                        [compare_table.table_name]
                    )
                    if len(invalid_identifier) > 0:
                        raise ValueError(
                            f"Not a valid snowflake identifier {', '.join(invalid_identifier)}"
                        )

                    for key, val in replacements.items():
                        statement = statement.replace(key, val)
                    failed_checks_sql.append((check.name, statement, {}))
        return failed_checks_sql


class AgnosticStatsCheck(SqlDecoratoratedOperator):
    def __init__(
        self,
        checks: List[OutlierCheck],
        main_table: Table,
        compare_table: Table,
        max_rows_returned: int,
        **kwargs,
    ):
        """
        Operator to run Statistical checks on tables.

        :param main_table: main table
        :type main_table: table object
        :param compare_table: table to be compared
        :type compare_table: table object
        :param checks: check class object, which represent boolean expression
        :type checks: Check
        :param max_rows_returned: number of row returned if the check fails.
        :type max_rows_returned: int
        """

        self.main_table = main_table
        self.compare_table = compare_table
        self.max_rows_returned = max_rows_returned
        self.conn_id = main_table.conn_id
        self.checks = checks
        self.database = main_table.database

        task_id = main_table.table_name + "_" + "stats_check"

        def null_function():
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            conn_id=main_table.conn_id,
            database=main_table.database,
            schema=main_table.schema,
            warehouse=main_table.warehouse,
            task_id=task_id,
            op_args=(),
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        conn_type = BaseHook.get_connection(self.conn_id).conn_type  # type: ignore
        self.handler = lambda curr: curr.fetchall()
        checkHandler = ChecksHandler(self.checks, conn_type)
        self.sql, self.parameters = checkHandler.prepare_comparison_sql(
            main_table=self.main_table, compare_table=self.compare_table
        )
        results = super().execute(context)
        failed_checks = checkHandler.evaluate_results(
            results, self.max_rows_returned, conn_type
        )
        if len(failed_checks) > 0:
            failed_check_sql = checkHandler.prepare_failed_checks_results(
                conn_type=conn_type,
                main_table=self.main_table,
                compare_table=self.compare_table,
                failed_checks=failed_checks,
                max_rows_returned=self.max_rows_returned,
            )
            failed_values = {}
            for check_query in failed_check_sql:
                check, self.sql, self.parameters = check_query
                results = super().execute(context)
                failed_values[check] = results

            raise ValueError("Stats Check Failed. {}".format(failed_values))


def stats_check(
    main_table: Table,
    compare_table: Table,
    checks: List[OutlierCheck] = [],
    max_rows_returned: int = 100,
):
    """
    :param main_table: main table
    :type main_table: table object
    :param compare_table: table to be compared
    :type compare_table: table object
    :param checks: check class object, which represent boolean expression
    :type checks: Check
    :param max_rows_returned: number of row returned if the check fails.
    :type max_rows_returned: int
    """

    return AgnosticStatsCheck(
        main_table=main_table,
        compare_table=compare_table,
        checks=checks,
        max_rows_returned=max_rows_returned,
    )
