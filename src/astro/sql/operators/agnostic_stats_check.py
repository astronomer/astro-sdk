from distutils import log as logger
from os import name, stat
from typing import Dict, List

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from sqlalchemy.sql.expression import table
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.schema import Table

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator
from astro.sql.table import Table


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
                for key, val in replacements.items():
                    stats_query = stats_query.replace(key, val)
                select_expressions.append(stats_query)

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
            for column in check.columns_map:
                main_table_col = column
                compare_table_col = check.columns_map[column]
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
            for column in check.columns_map:
                main_table_col = column
                compare_table_col = check.columns_map[column]
                statement = "CASE WHEN {column_sql} THEN 1 ELSE 0 END as {check_name}"
                replacements = {
                    "{column_sql}": self.prepare_column_snowflake_sql(check),
                    "{check_name}": check.name,
                }
                for key, val in replacements.items():
                    statement = statement.replace(key, val)
                cases.append(statement)
        return ",".join(cases)

    def prepare_comparison_sql(self, main_table: Table, compare_table: Table):
        statement = ""
        if self.conn_type == "postgres":
            hook = PostgresHook(postgres_conn_id=main_table.conn_id)
            statement = (
                sql.SQL(
                    "SELECT {cases}, {compare_table}.*  FROM {main_stats}, {compare_table}"
                )
                .format(
                    cases=self.prepare_cases_postgres_sql(),
                    main_stats=self.prepare_main_stats_postgres_sql(main_table),
                    compare_table=sql.Identifier(compare_table.table_name),
                )
                .as_string(hook.get_conn())
            )
        elif self.conn_type == "snowflake":
            statement = "SELECT {cases}, {compare_table}.*  FROM {main_stats}, Identifier('{compare_table}')"
            replacements = {
                "{cases}": self.prepare_cases_snowflake_sql(),
                "{compare_table}": compare_table.table_name,
                "{main_stats}": self.prepare_main_stats_snowflake_sql(main_table),
            }
            for key, val in replacements.items():
                statement = statement.replace(key, val)
        else:
            raise ValueError("Unsupported connection type")
        return statement, {}

    def check_snowflake_results(self, row, check, index):
        return row[check.name.upper()]

    def check_postgres_results(self, row, check, index):
        return row[index]

    def evaluate_results(self, rows, max_rows_returned, conn_type):

        check_results = {
            "postgres": self.check_postgres_results,
            "snowflake": self.check_snowflake_results,
        }[conn_type]

        total_rows = len(rows)
        failed_checks = {check.name: {"count": 0, "rows": []} for check in self.checks}
        for row in rows:
            index = 0
            for check in self.checks:
                if check_results(row, check, index):
                    failed_checks[check.name]["count"] += 1
                    if len(failed_checks[check.name]["rows"]) < max_rows_returned:
                        failed_checks[check.name]["rows"].append(row)
                index += 1

        for check in self.checks:
            count = failed_checks[check.name]["count"]
            if (count / total_rows) <= check.threshold:
                failed_checks.pop(check.name)
            else:
                failed_checks[check.name] = failed_checks[check.name]["rows"]
        return failed_checks


class AgnosticStatsCheck(SqlDecoratoratedOperator):
    def __init__(
        self,
        checks: List[OutlierCheck],
        main_table: Table,
        compare_table: Table,
        max_rows_returned: int,
        **kwargs
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
        print("results : ", results)
        results = checkHandler.evaluate_results(
            results, self.max_rows_returned, conn_type
        )
        if len(results) > 0:
            raise ValueError("Stats Check Failed. {}".format(results))


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
