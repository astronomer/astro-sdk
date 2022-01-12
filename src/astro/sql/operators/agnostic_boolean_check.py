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
from astro.utils.snowflake_merge_func import is_valid_snow_identifier


class Check:
    def __init__(
        self,
        name: str,
        expression: str,
        threshold: float = 0,
    ) -> None:
        self.name = name
        self.expression = expression
        self.threshold = threshold

    def get_expression(self, conn_type: str):
        return {
            "postgres": self.get_postgres_expression(),
            "snowflake": self.get_snowflake_expression(),
        }[conn_type]

    def get_postgres_expression(self):
        return sql.SQL("CASE WHEN {expression} THEN 0 ELSE 1 END AS {name}").format(
            expression=sql.SQL(self.expression), name=sql.Identifier(self.name)
        )

    def get_postgres_result(self):
        return sql.SQL("CAST(SUM({name}) as float) / COUNT(*) as {result}").format(
            name=sql.Identifier(self.name), result=sql.Identifier(self.name + "_result")
        )

    def get_snowflake_expression(self):
        if not is_valid_snow_identifier(name):
            raise ValueError("Not a valid snowflake identifier {}".format(name))
        return "CASE WHEN {}  THEN 0 ELSE 1 END AS {}".format(
            self.expression, self.name
        )

    def get_snowflake_result(self):
        if not is_valid_snow_identifier(name):
            raise ValueError("Not a valid snowflake identifier {}".format(name))
        return 'CAST(SUM({}) as float) / COUNT(*) as "{}"'.format(
            self.name, self.name + "_result"
        )


class AgnosticBooleanCheck(SqlDecoratoratedOperator):
    def __init__(
        self,
        checks: List[Check],
        table: Table,
        max_rows_returned: int,
        **kwargs,
    ):
        """
        :param table: table name
        :type table: str
        :param checks: check class object, which represent boolean expression
        :type checks: Check
        :param max_rows_returned: number of row returned if the check fails.
        :type max_rows_returned: int
        """

        self.table = table
        self.max_rows_returned = max_rows_returned
        self.conn_id = table.conn_id
        self.checks = checks
        self.database = table.database

        task_id = table.table_name + "_" + "boolean_check"

        def null_function():
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            conn_id=table.conn_id,
            database=table.database,
            schema=table.schema,
            warehouse=table.warehouse,
            task_id=task_id,
            op_args=(),
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Dict):
        conn_type = BaseHook.get_connection(self.conn_id).conn_type  # type: ignore
        postgres = "postgres"
        snowflake = "snowflake"
        try:
            execute_boolean_checks = {
                postgres: AgnosticBooleanCheck.execute_postgres_boolean_checks,
                snowflake: AgnosticBooleanCheck.execute_snowflake_boolean_checks,
            }[conn_type]
            get_failed_checks = {
                postgres: self.get_postgres_failed_checks,
                snowflake: self.get_snowflake_failed_checks,
            }[conn_type]
            prep_results = {
                postgres: self.postgres_prep_results,
                snowflake: self.snowflake_prep_results,
            }[conn_type]
        except KeyError:
            raise ValueError(f"Please specify a postgres or snowflake conn id.")

        self.handler = lambda curr: curr.fetchall()

        self.sql, self.parameters = execute_boolean_checks(
            self.table.table_name, self.checks, self.conn_id  # type: ignore
        )

        results = super().execute(context)
        failed_checks_names, failed_checks_index = get_failed_checks(results)
        if len(failed_checks_index) > 0:
            self.sql, self.parameters = prep_results(failed_checks_index)
            failed_rows = super().execute(context)
            logger.error("Failed rows {}".format(failed_rows))
            raise ValueError(
                "Some of the check(s) have failed {}".format(
                    ",".join(failed_checks_names)
                )
            )

        return table

    def get_postgres_failed_checks(self, results):
        failed_check_name = []
        failed_check_index = []

        for index in range(len(self.checks)):
            if self.checks[index].threshold < results[0][index]:
                failed_check_name.append(self.checks[index].name)
                failed_check_index.append(index)
        return failed_check_name, failed_check_index

    def get_snowflake_failed_checks(self, results):
        failed_check_name = []
        failed_check_index = []

        for index in range(len(self.checks)):
            if (
                self.checks[index].threshold
                < results[0][self.checks[index].name + "_result"]
            ):
                failed_check_name.append(self.checks[index].name)
                failed_check_index.append(index)
        return failed_check_name, failed_check_index

    @staticmethod
    def execute_postgres_boolean_checks(table: str, checks: List[Check], conn_id: str):
        statement = (
            "SELECT {results} FROM (SELECT {expressions} From {table}) as temp_table"
        )
        expressions = sql.SQL(",").join(
            [check.get_postgres_expression() for check in checks]
        )
        results = sql.SQL(",").join([check.get_postgres_result() for check in checks])

        hook = PostgresHook(postgres_conn_id=conn_id)

        statement = (
            sql.SQL(statement)
            .format(
                results=results, expressions=expressions, table=sql.Identifier(table)
            )
            .as_string(hook.get_conn())
        )
        return statement, {}

    @staticmethod
    def execute_snowflake_boolean_checks(table: str, checks: List[Check], conn_id: str):
        statement = "SELECT {results} FROM (SELECT {expressions} From Identifier(%(table)s) as temp_table)"

        statement = statement.replace("{table}", table)
        statement = statement.replace(
            "{expressions}",
            ",".join([check.get_snowflake_expression() for check in checks]),
        )
        statement = statement.replace(
            "{results}", ",".join([check.get_snowflake_result() for check in checks])
        )
        return statement, {"table": table}

    def postgres_prep_results(self, results):
        statement = "SELECT * from {table} WHERE {checks} LIMIT {max_rows_returned}"
        failed_checks = [sql.SQL(self.checks[index].expression) for index in results]
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        statement = (
            sql.SQL(statement)
            .format(
                table=sql.Identifier(self.table.table_name),
                checks=sql.SQL("AND").join(failed_checks),
                max_rows_returned=sql.SQL(str(self.max_rows_returned)),
            )
            .as_string(hook.get_conn())
        )
        return statement, {}

    def snowflake_prep_results(self, results):
        statement = (
            "SELECT * from Identifier(%(table)s) WHERE {expressions} LIMIT {limit}"
        )
        statement = statement.replace(
            "{expressions}",
            "AND".join([self.checks[index].expression for index in results]),
        )
        statement = statement.replace("{limit}", str(self.max_rows_returned))
        return statement, {"table": self.table}


def wrap_identifier(inp):
    return "Identifier(%(" + inp + ")s)"


def boolean_check(table: Table, checks: List[Check] = [], max_rows_returned: int = 100):
    """
    :param table: table name
    :type table: str
    :param checks: check class object, which represent boolean expression
    :type checks: Check
    :param max_rows_returned: number of row returned if the check fails.
    :type max_rows_returned: int
    """

    return AgnosticBooleanCheck(
        table=table, checks=checks, max_rows_returned=max_rows_returned
    )
