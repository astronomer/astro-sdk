from distutils import log as logger
from typing import Dict, List

from sqlalchemy import FLOAT, and_, cast, column, func, select, text
from sqlalchemy.sql.expression import table

from astro.sql.operators.sql_decorator import SqlDecoratoratedOperator
from astro.sql.table import Table


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

    def get_expression(self):
        return text(f"CASE WHEN {self.expression} THEN 0 ELSE 1 END AS {self.name}")

    def get_result(self):
        return cast(func.sum(column(self.name)), FLOAT) / func.count().label(
            self.name + "_result"
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
        :param table: table to check
        :type table: Table
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
        self.parameters = {}
        self.sql = AgnosticBooleanCheck.prep_boolean_checks_query(
            self.table, self.checks
        )

        results = super().execute(context)
        results = results.fetchall()
        failed_checks_names, failed_checks_index = self.get_failed_checks(results)
        if len(failed_checks_index) > 0:
            self.parameters = {}
            self.sql = self.prep_results(failed_checks_index)
            failed_rows = super().execute(context)
            failed_rows = failed_rows.fetchall()
            logger.error("Failed rows {}".format(failed_rows))
            raise ValueError(
                "Some of the check(s) have failed {}".format(
                    ",".join(failed_checks_names)
                )
            )

        return table

    def get_failed_checks(self, results):
        failed_check_name = []
        failed_check_index = []

        for index in range(len(self.checks)):
            if self.checks[index].threshold < results[0][index]:
                failed_check_name.append(self.checks[index].name)
                failed_check_index.append(index)
        return failed_check_name, failed_check_index

    def prep_boolean_checks_query(table: Table, checks: List[Check]):
        temp_table = (
            select([check.get_expression() for check in checks])
            .select_from(text(table.qualified_name()))
            .alias("check_table")
        )
        return select([check.get_result() for check in checks]).select_from(temp_table)

    def prep_results(self, results):
        return (
            select(["*"])
            .select_from(text(self.table.qualified_name()))
            .where(and_(*[text(self.checks[index].expression) for index in results]))
            .limit(self.max_rows_returned)
        )


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
