from distutils import log as logger
from os import name
from typing import Dict, List

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from sqlalchemy.sql.expression import table
from sqlalchemy.sql.functions import Function
from sqlalchemy.sql.schema import Table

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator


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
        return sql.SQL("CASE WHEN {expression}  THEN 0 ELSE 1 END AS {name}").format(
            expression=sql.SQL(self.expression), name=sql.SQL(self.name)
        )

    def get_postgres_result(self):
        return sql.SQL("CAST(SUM({name}) as float) / COUNT(*) as {result}").format(
            name=sql.SQL(self.name), result=sql.SQL(self.name + "_result")
        )

    def get_snowflake_expression(self):
        return "CASE WHEN {}  THEN 0 ELSE 1 END AS {}".format(
            self.expression, self.name
        )

    def get_snowflake_result(self):
        return "CAST(SUM({}) as float) / COUNT(*) as {}".format(
            self.name, self.name + "_result"
        )


class AgnosticBooleanCheck(SqlDecoratoratedOperator):
    def __init__(
        self,
        database: str,
        checks: List[Check],
        table: str,
        max_rows_returned: int,
        conn_id: str = "",
        **kwargs,
    ):
        """
        :param table: table name
        :type table: str
        :param database: database name
        :type database: str
        :param checks: check class object, which represent boolean expression
        :type checks: Check
        :param max_rows_returned: number of row returned if the check fails.
        :type max_rows_returned: int
        """

        self.table = table
        self.max_rows_returned = max_rows_returned
        self.conn_id = conn_id
        self.checks = checks

        task_id = table + "_" + "boolean_check"

        def null_function():
            pass

        super().__init__(
            raw_sql=True,
            parameters={},
            conn_id=conn_id,
            task_id=task_id,
            op_args=(),
            python_callable=null_function,
            database=database,
            **kwargs,
        )

    def execute(self, context: Dict):
        conn_type = BaseHook.get_connection(self.conn_id).conn_type  # type: ignore
        self.handler = lambda curr: curr.fetchall()
        if conn_type == "postgres":
            self.sql = AgnosticBooleanCheck.postgres_boolean_check_func(
                self.table, self.checks, self.conn_id  # type: ignore
            )

            results = super().execute(context)
            failed_checks_names, failed_checks_index = self.get_failed_checks(results)
            if len(failed_checks_index) > 0:
                self.sql = self.postgres_prep_results(failed_checks_index)
                print("self.sql : ", self.sql)
                failed_rows = super().execute(context)
                logger.error("Failed rows {}".format(failed_rows))
                raise ValueError(
                    "Some of the check(s) have failed {}".format(
                        ",".join(failed_checks_names)
                    )
                )

        elif conn_type == "snowflake":
            (
                self.sql,
                self.parameters,
            ) = AgnosticBooleanCheck.snowflake_boolean_check_func(
                self.table, self.checks
            )
            print("self.sql : ", self.sql)
            results = super().execute(context)
            print("results : ", results)
        else:
            raise ValueError(f"Please specify a postgres or snowflake conn id.")

    def get_failed_checks(self, results):
        failed_check_name = []
        failed_check_index = []
        for index in range(len(self.checks)):
            if self.checks[index].threshold < results[0][index]:
                failed_check_name.append(self.checks[index].name)
                failed_check_index.append(index)
        return failed_check_name, failed_check_index

    @staticmethod
    def postgres_boolean_check_func(table: str, checks: List[Check], conn_id: str):
        statement = (
            "SELECT {results} FROM (SELECT {expressions} From {table}) as temp_table"
        )
        expressions = sql.SQL(",").join(
            [check.get_postgres_expression() for check in checks]
        )
        results = sql.SQL(",").join([check.get_postgres_result() for check in checks])

        hook = PostgresHook(postgres_conn_id=conn_id)

        return (
            sql.SQL(statement)
            .format(
                results=results, expressions=expressions, table=sql.Identifier(table)
            )
            .as_string(hook.get_conn())
        )

    @staticmethod
    def snowflake_boolean_check_func(table: str, checks: List[Check]):
        statement = "SELECT %(results)s FROM (SELECT %(expressions)s From boolean_check_test) as temp_table"
        expressions = ",".join([check.get_snowflake_expression() for check in checks])
        results = ",".join([check.get_snowflake_result() for check in checks])

        statement = "SELECT * FROM IDENTIFIER(%(table)s)"
        return statement, {"table": "boolean_check_test"}

        # return statement, {
        #     "results" : results,
        #     "expressions" : expressions
        # }

    def postgres_prep_results(self, results):
        statement = "SELECT * from {table} WHERE {checks} LIMIT {max_rows_returned}"
        failed_checks = [sql.SQL(self.checks[index].expression) for index in results]
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        return (
            sql.SQL(statement)
            .format(
                table=sql.Identifier(self.table),
                checks=sql.SQL("AND").join(failed_checks),
                max_rows_returned=sql.SQL(str(self.max_rows_returned)),
            )
            .as_string(hook.get_conn())
        )

    @staticmethod
    def snowflake_prep_results(self, results):
        statement = "SELECT * from {} WHERE {} LIMIT {}"
        failed_checks = [self.checks[index].expression for index in results]
        return statement.format(
            wrap_identifier(self.table),
            "AND".join(failed_checks),
            str(self.max_rows_returned),
        )


def wrap_identifier(inp):
    return "Identifier(%(" + inp + ")s)"


def boolean_check(
    table: Table,
    database: str,
    checks: List[Check],
    max_rows_returned: int,
    conn_id: str = "",
):
    """
    :param table: table name
    :type table: str
    :param database: database name
    :type database: str
    :param checks: check class object, which represent boolean expression
    :type checks: Check
    :param max_rows_returned: number of row returned if the check fails.
    :type max_rows_returned: int
    """

    return AgnosticBooleanCheck(
        table=table,
        checks=checks,
        database=database,
        max_rows_returned=max_rows_returned,
        conn_id=conn_id,
    )
