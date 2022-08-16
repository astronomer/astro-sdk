from typing import Any, Callable, Dict, Iterable, Mapping, Optional, Union

try:
    from airflow.decorators.base import TaskDecorator, task_decorator_factory
except ImportError:
    from airflow.decorators.base import task_decorator_factory
    from airflow.decorators import _TaskDecorator as TaskDecorator

from astro.sql.operators.base_decorator import BaseSQLDecoratedOperator


class TransformOperator(BaseSQLDecoratedOperator):
    """
    Given a SQL statement and (optional) tables, execute the SQL statement and output
    the result into a SQL table.
    """

    def execute(self, context: Dict):
        super().execute(context)

        self.database_impl.create_schema_if_needed(self.output_table.metadata.schema)
        self.database_impl.drop_table(self.output_table)
        self.database_impl.create_table_from_select_statement(
            statement=self.sql,
            target_table=self.output_table,
            parameters=self.parameters,
        )
        return self.output_table


def transform(
    python_callable: Optional[Callable] = None,
    conn_id: str = "",
    parameters: Optional[Union[Mapping, Iterable]] = None,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    **kwargs: Any,
) -> TaskDecorator:
    """
    Given a python function that returns a SQL statement and (optional) tables, execute the SQL statement and output
    the result into a SQL table.

    Use this function as a decorator like so:

    .. code-block:: python

      @transform
      def my_sql_statement(table1: Table, table2: Table) -> Table:
          return "SELECT * FROM {{table1}} JOIN {{table2}}"

    In this example, by identifying the parameters as `Table` objects, astro knows to automatically convert those
    objects into tables (if they are, for example, a dataframe). Any type besides table will lead astro to assume
    you do not want the parameter converted.

    You can also pass parameters into the query like so

     .. code-block:: python

      @transform
      def my_sql_statement(table1: Table, table2: Table, execution_date) -> Table:
          return "SELECT * FROM {{table1}} JOIN {{table2}} WHERE date > {{exec_date}}", {
              "exec_date": execution_date
          }

    You can also pass SQL file to the sql parameter in the transform decorator

     .. code-block:: python

      @transform(sql="/path/to/sql/sql_file.sql")
      def my_sql_file(table1: Table):
          return

    :param python_callable: This parameter is filled in automatically when you use the transform function as a decorator
        This is where the python function gets passed to the wrapping function
    :param conn_id: Connection ID for the database you want to connect to. If you do not pass in a value for this object
        we can infer the connection ID from the first table passed into the python_callable function.
        (required if there are no table arguments)
    :param parameters: parameters to pass into the SQL query
    :param database: Database within the SQL instance you want to access. If left blank we will default to the
        table.metatadata.database in the first Table passed to the function (required if there are no table arguments)
    :param schema: Schema within the SQL instance you want to access. If left blank we will default to the
        table.metatadata.schema in the first Table passed to the function (required if there are no table arguments)
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    :return: Transform functions return a ``Table`` object that can be passed to future tasks.
        This table will be either an auto-generated temporary table,
        or will overwrite a table given in the `output_table` parameter.
    """

    kwargs.update(
        {
            "conn_id": conn_id,
            "parameters": parameters,
            "database": database,
            "schema": schema,
            "handler": None,
        }
    )
    return task_decorator_factory(
        python_callable=python_callable,
        multiple_outputs=False,
        decorated_operator_class=TransformOperator,
        **kwargs,
    )
