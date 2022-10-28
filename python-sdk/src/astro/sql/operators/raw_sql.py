from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any, Callable

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from astro import settings
from astro.exceptions import IllegalLoadToDatabaseException
from astro.sql.operators.base_decorator import BaseSQLDecoratedOperator
from astro.utils.typing_compat import Context


class RawSQLOperator(BaseSQLDecoratedOperator):
    """
    Given a SQL statement, (optional) tables and a (optional) function, execute the SQL statement
    and apply the function to the results, returning the result of the function.

    Disclaimer: this could potentially trash the XCom Database, depending on the XCom backend used
    and on the SQL statement/function declared by the user.
    """

    def execute(self, context: Context) -> Any:
        super().execute(context)

        result = self.database_impl.run_sql(sql=self.sql, parameters=self.parameters)
        if self.response_size == -1 and not settings.IS_CUSTOM_XCOM_BACKEND:
            logging.warning(
                "Using `run_raw_sql` without `response_size` can result in excessive amount of data being recorded "
                "to the Airflow metadata database, leading to issues to the orchestration of tasks. It is possible to "
                "avoid this problem by either setting `response_size` to a small integer or by using a custom XCom "
                "backend."
            )

        if self.handler:
            response = self.handler(result)
            if 0 <= self.response_limit < len(response):
                raise IllegalLoadToDatabaseException()  # pragma: no cover
            if self.response_size >= 0:
                return response[: self.response_size]
            else:
                return response
        else:
            return None


def run_raw_sql(
    python_callable: Callable | None = None,
    conn_id: str = "",
    parameters: Mapping | Iterable | None = None,
    database: str | None = None,
    schema: str | None = None,
    handler: Callable | None = None,
    response_size: int = settings.RAW_SQL_MAX_RESPONSE_SIZE,
    **kwargs: Any,
) -> TaskDecorator:
    """
    Given a python function that returns a SQL statement and (optional) tables, execute the SQL statement and output
    the result into a SQL table.

    Use this function as a decorator like so:


    .. code-block:: python

      @run_raw_sql
      def my_sql_statement(table1: Table) -> Table:
          return "DROP TABLE {{table1}}"

    In this example, by identifying parameters as ``Table`` objects, astro knows to automatically convert those
    objects into tables (if they are, for example, a dataframe). Any type besides table will lead astro to assume
    you do not want the parameter converted.

    Please note that the ``run_raw_sql`` function will not create a temporary table. It will either return the
    result of a provided ``handler`` function or it will not return anything at all.


    :param python_callable: This parameter is filled in automatically when you use the transform function as a
        decorator. This is where the python function gets passed to the wrapping function
    :param conn_id: Connection ID for the database you want to connect to.
        If you do not pass in a value for this object we can infer the connection ID from the first table
        passed into the python_callable function. (required if there are no table arguments)
    :param parameters: parameters to pass into the SQL query
    :param database: Database within the SQL instance you want to access. If left blank we will default to the
        table.metatadata.database in the first Table passed to the function
        (required if there are no table arguments)
    :param schema: Schema within the SQL instance you want to access. If left blank we will default to the
        table.metatadata.schema in the first Table passed to the function
        (required if there are no table arguments)
    :param handler: Handler function to process the result of the SQL query. For more information please consult
        https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Result
    :param response_size: Used to trim the responses returned to avoid trashing the Airflow DB.
        The default value is -1, which means the response is not changed. Otherwise, if the response is a list,
        returns up to the desired amount of items. If the response is a string, trims it to the desired size.
    :param kwargs:
    :return: By default returns None unless there is a handler function,
        in which case returns the result of the handler
    """

    kwargs.update(
        {
            "conn_id": conn_id,
            "parameters": parameters,
            "database": database,
            "schema": schema,
            "handler": handler,
            "response_size": response_size,
        }
    )
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=RawSQLOperator,
        **kwargs,
    )
