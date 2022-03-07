from distutils import log as logger
from typing import Dict, List

from airflow.hooks.base import BaseHook
from sqlalchemy import FLOAT, and_, cast, column, func, select, text
from sqlalchemy.sql.expression import table as sqlatable

from astro.sql.operators.sql_decorator import SqlDecoratedOperator
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


class AgnosticBooleanCheck(SqlDecoratedOperator):
    template_fields = ("table",)

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

        def handler_func(results):
            return results.fetchall()

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
            handler=handler_func,
            **kwargs,
        )

    def execute(self, context: Dict):
        conn = BaseHook.get_connection(self.conn_id)
        self.parameters = {"table": self.table}
        self.sql = self.prep_boolean_checks_query(self.table, self.checks, context)

        results = super().execute(context)
        failed_checks_names, failed_checks_index = self.get_failed_checks(results)
        if len(failed_checks_index) > 0:
            self.parameters = {"table": self.table, "limit": self.max_rows_returned}
            self.sql = self.prep_results(failed_checks_index)
            failed_rows = super().execute(context)
            logger.error("Failed rows %s", failed_rows)
            raise ValueError(
                "Some of the check(s) have failed %s", ",".join(failed_checks_names)
            )

        return self.table

    def get_failed_checks(self, results):
        failed_check_name = []
        failed_check_index = []

        for index in range(len(self.checks)):
            if self.checks[index].threshold < results[0][index]:
                failed_check_name.append(self.checks[index].name)
                failed_check_index.append(index)
        return failed_check_name, failed_check_index

    @staticmethod
    def get_expression(expression, name):
        return text(f"CASE WHEN {expression} THEN 0 ELSE 1 END AS {name}")

    def prep_boolean_checks_query(
        self, table: Table, checks: List[Check], context: Dict
    ):

        sqla_checks_object = []
        context = self._add_templates_to_context(context)
        for check in checks:
            prepared_exp = self.render_template(check.expression, context)
            sqla_checks_object.append(
                AgnosticBooleanCheck.get_expression(prepared_exp, check.name)
            )

        temp_table = (
            select(sqla_checks_object)
            .select_from(text(table.qualified_name()))
            .alias("check_table")
        )
        return select([check.get_result() for check in checks]).select_from(temp_table)

    def prep_results(self, results):
        return (
            select(["*"])
            .select_from(text("{{table}}"))
            .where(and_(*[text(self.checks[index].expression) for index in results]))
            .limit("{{limit}}")
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
