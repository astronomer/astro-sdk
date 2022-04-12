from typing import Optional

from airflow.utils.context import Context

from astro.sql.operators.sql_decorator import SqlDecoratedOperator
from astro.sql.table import Table
from astro.utils.task_id_helper import get_unique_task_id


class AgnosticAggregateCheck(SqlDecoratedOperator):
    template_fields = ("table",)

    def __init__(
        self,
        table: Table,
        check: str,
        greater_than: Optional[float] = None,
        less_than: Optional[float] = None,
        equal_to: Optional[float] = None,
        **kwargs,
    ):
        """Validate that a table has expected aggregation value.
        Range specified by greater_than and/or less_than is inclusive - [greater_than, less_than] or
        equal_to can be used to check a value.

        :param table: table name
        :param check: SQL statement
        :param greater_than: min expected value
        :param less_than: max expected value
        :param equal_to: expected value
        :param conn_id: connection id
        :param database: database name
        """
        self.table = table
        self.check = check
        self.greater_than = greater_than
        self.less_than = less_than
        self.equal_to = equal_to

        if less_than is None and greater_than is None and equal_to is None:
            raise ValueError(
                "Please provide one or more of these options: less_than, greater_than, equal_to"
            )

        if (
            less_than is not None
            and greater_than is not None
            and less_than < greater_than
        ):
            raise ValueError(
                "less_than should be greater than or equal to greater_than."
            )

        task_id = get_unique_task_id("aggregate_check")

        def null_function():
            pass

        def handler_func(result):
            return result.fetchone()[0]

        super().__init__(
            raw_sql=True,
            parameters={},
            task_id=task_id,
            op_args=(),
            handler=handler_func,
            python_callable=null_function,
            **kwargs,
        )

    def execute(self, context: Context) -> Table:
        self.conn_id = self.table.conn_id
        self.database = self.table.database
        self.sql = self.check
        self.parameters = {"table": self.table}

        query_result = super().execute(context)
        if not isinstance(query_result, int) and not isinstance(query_result, float):
            raise ValueError(
                "The aggregate check query should only return a numeric value."
            )

        if self.equal_to is not None and self.equal_to != query_result:
            raise ValueError(
                f"Check Failed: query result value {query_result} not equal to {self.equal_to}."
            )
        elif (
            self.less_than is not None
            and self.greater_than is not None
            and (self.greater_than > query_result or query_result > self.less_than)
        ):
            raise ValueError(
                "Check Failed: query result value {} not in range from {} to {}.".format(
                    query_result, self.greater_than, self.less_than
                )
            )
        elif self.less_than is not None and self.less_than < query_result:
            raise ValueError(
                f"Check Failed: query result value {query_result} not less than {self.less_than}."
            )
        elif self.greater_than is not None and self.greater_than > query_result:
            raise ValueError(
                "Check Failed: query result value {} not greater than {}.".format(
                    query_result, self.greater_than
                )
            )
        return self.table


def aggregate_check(
    table: Table,
    check: str,
    greater_than: Optional[float] = None,
    less_than: Optional[float] = None,
    equal_to: Optional[float] = None,
) -> AgnosticAggregateCheck:
    """
    :param table: table name
    :type table: str
    :param check: SQL statement
    :type check: str
    :param greater_than: min expected rows
    :type greater_than: int
    :param less_than: max expected rows
    :type less_than: int
    :param conn_id: connection id,
    :type conn_id: str
    :param database: database name,
    :type database: str
    """
    return AgnosticAggregateCheck(
        table=table,
        check=check,
        greater_than=greater_than,
        less_than=less_than,
        equal_to=equal_to,
    )
