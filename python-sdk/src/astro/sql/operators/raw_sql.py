from __future__ import annotations

import logging
from collections.abc import Iterable, Mapping
from typing import Any, Callable

try:
    from airflow.decorators.base import TaskDecorator
except ImportError:
    from airflow.decorators import _TaskDecorator as TaskDecorator

import airflow
import pandas as pd
from airflow.decorators.base import task_decorator_factory
from sqlalchemy.engine import ResultProxy

if airflow.__version__ >= "2.3":
    from sqlalchemy.engine.row import LegacyRow as SQLAlcRow
else:
    from sqlalchemy.engine.result import RowProxy as SQLAlcRow

from astro import settings
from astro.constants import RunRawSQLResultFormat
from astro.dataframes.pandas import PandasDataframe
from astro.exceptions import IllegalLoadToDatabaseException
from astro.sql.operators.base_decorator import BaseSQLDecoratedOperator
from astro.utils.compat.typing import Context


class RawSQLOperator(BaseSQLDecoratedOperator):
    """
    Given a SQL statement, (optional) tables and a (optional) function, execute the SQL statement
    and apply the function to the results, returning the result of the function.

    Disclaimer: this could potentially trash the XCom Database, depending on the XCom backend used
    and on the SQL statement/function declared by the user.
    """

    def __init__(
        self,
        results_format: RunRawSQLResultFormat | None = None,
        fail_on_empty: bool = False,
        **kwargs: Any,
    ):
        super().__init__(**kwargs)

        # add the results_format and fail_on_empty to the kwargs
        self.results_format = results_format
        self.fail_on_empty = fail_on_empty

    def execute(self, context: Context) -> Any:
        super().execute(context)

        self.handler = self.get_handler()
        result = self.database_impl.run_sql(
            sql=self.sql,
            parameters=self.parameters,
            handler=self.handler,
            query_modifier=self.query_modifier,
        )
        if self.response_size == -1 and not settings.IS_CUSTOM_XCOM_BACKEND:
            logging.warning(
                "Using `run_raw_sql` without `response_size` can result in excessive amount of data being recorded "
                "to the Airflow metadata database, leading to issues to the orchestration of tasks. It is possible to "
                "avoid this problem by either setting `response_size` to a small integer or by using a custom XCom "
                "backend."
            )

        if self.handler:
            response = self.make_row_serializable(result)
            if 0 <= self.response_limit < len(response):
                raise IllegalLoadToDatabaseException()  # pragma: no cover
            if self.response_size >= 0:
                resp = response[: self.response_size]
                # We do the following is slicing pandas dataframe creates a pd.Dataframe object
                # which isn't serializable
                # TODO: We could add a __getitem__ to PandasDataframe object
                return PandasDataframe.from_pandas_df(resp) if isinstance(resp, pd.DataFrame) else resp
            else:
                return response

        # if no results_format or handler is specified, we don't return results
        return None

    @staticmethod
    def make_row_serializable(rows: Any) -> Any:
        """
        Convert rows to a serializable format
        """
        if not settings.NEED_CUSTOM_SERIALIZATION:
            return rows
        if isinstance(rows, PandasDataframe):
            return rows
        if isinstance(rows, Iterable):
            return [SdkLegacyRow.from_legacy_row(r) if isinstance(r, SQLAlcRow) else r for r in rows]
        return rows

    @staticmethod
    def results_as_list(results: ResultProxy) -> list:
        """
        Convert the result of a SQL query to a list
        """
        data = []
        for result in results.fetchall():
            data.append(result.values())
        return data

    @staticmethod
    def results_as_pandas_dataframe(result: ResultProxy) -> pd.DataFrame:
        """
        Convert the result of a SQL query to a pandas dataframe
        """
        return PandasDataframe(result.fetchall(), columns=result.keys())

    def get_results_format_handler(self, results_format: str):
        return getattr(self, f"results_as_{results_format}")

    def get_handler(self):
        # if a user specifies a results_format, we will return the results in that format
        # note that it takes precedence over the handler
        if self.results_format:
            self.handler = self.get_results_format_handler(results_format=self.results_format)
            logging.info("Provided 'handler' will be overridden when 'results_format' is given.")
        if self.handler:
            self.handler = self.get_wrapped_handler(
                fail_on_empty=self.fail_on_empty, conversion_func=self.handler
            )
        return self.handler

    @staticmethod
    def get_wrapped_handler(fail_on_empty: bool, conversion_func: Callable):
        # if fail_on_empty is set to False, wrap in a try/except block
        # this is because the conversion function will fail if the result is empty
        # due to sqlalchemy failing when there are no results
        if not fail_on_empty:

            def handle_exceptions(result):
                try:
                    return conversion_func(result)
                except Exception as e:  # skipcq: PYL-W0703
                    logging.info("Exception %s handled since 'fail_on_empty' parameter was passed", str(e))
                    return None

            return handle_exceptions

        # otherwise, just return the result
        return conversion_func


class SdkLegacyRow(SQLAlcRow):
    version: int = 1

    def serialize(self):
        return {"key_map": self._keymap, "key_style": self._key_style, "data": self._data}

    @staticmethod
    def deserialize(data: dict, version: int):  # skipcq: PYL-W0613
        return SdkLegacyRow(None, None, data["key_map"], data["key_style"], data["data"])

    @staticmethod
    def from_legacy_row(obj):
        return SdkLegacyRow(None, None, obj._keymap, obj._key_style, obj._data)  # skipcq: PYL-W0212


def run_raw_sql(
    python_callable: Callable | None = None,
    conn_id: str = "",
    parameters: Mapping | Iterable | None = None,
    database: str | None = None,
    schema: str | None = None,
    handler: Callable | None = None,
    results_format: RunRawSQLResultFormat | None = None,
    fail_on_empty: bool = False,
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
        table.metadata.database in the first Table passed to the function
        (required if there are no table arguments)
    :param schema: Schema within the SQL instance you want to access. If left blank we will default to the
        table.metadata.schema in the first Table passed to the function
        (required if there are no table arguments)
    :param handler: Handler function to process the result of the SQL query. For more information please consult
        https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Result
    :param results_format: Format of the results returned by the SQL query. Overrides the handler, if provided.
    :param fail_on_empty: If True and a results_format is provided, raises an exception if the query returns no results.
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
            "results_format": results_format,
            "fail_on_empty": fail_on_empty,
            "response_size": response_size,
        }
    )
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=RawSQLOperator,
        **kwargs,
    )
