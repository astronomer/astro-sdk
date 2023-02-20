from __future__ import annotations

from typing import Any

from airflow import XComArg
from sqlalchemy.sql.functions import Function

from astro.sql import TransformOperator
from astro.utils.compat.typing import Context


class NominalTransformOperator(TransformOperator):
    """
    Given a SQL statement and (optional) tables, execute the SQL statement and output
    the result into a SQL table.
    """

    def __init__(
        self,
        conn_id: str | None = None,
        parameters: dict | None = None,
        handler: Function | None = None,
        database: str | None = None,
        schema: str | None = None,
        response_limit: int = -1,
        response_size: int = -1,
        sql: str = "",
        task_id: str = "",
        **kwargs: Any,
    ):
        super().__init__(
            conn_id=conn_id,
            parameters=parameters,
            handler=handler,
            database=database,
            schema=schema,
            response_limit=response_limit,
            response_size=response_size,
            sql=sql,
            task_id=task_id,
            **kwargs,
        )

    def execute(self, context: Context):
        super().execute(context)
        # Skips the core execution of the operator but run all ancillary operations. This is useful for local run of
        # tasks where subsequent tasks in the DAG might need the output of this operator like XCOM results, but they
        # do not want to actually run the core logic and hence not cause effects outside the system.
        return self.output_table


def nominal_transform_file(
    file_path: str,
    conn_id: str = "",
    parameters: dict | None = None,
    database: str | None = None,
    schema: str | None = None,
    **kwargs: Any,
) -> XComArg:
    """
    This function returns a ``Table`` object that can be passed to future tasks from specified SQL file.
    Tables can be inserted via the parameters kwarg.

    :param file_path: File path for the SQL file you would like to parse. Can be an absolute path, or you can use a
        relative path if the `template_searchpath` variable is set in your DAG
    :param conn_id: Connection ID for the database you want to connect to. If you do not pass in a value for this object
        we can infer the connection ID from the first table passed into the python_callable function.
        (required if there are no table arguments)
    :param parameters: parameters to pass into the SQL query
    :param database: Database within the SQL instance you want to access. If left blank we will default to the
        table.metadata.database in the first Table passed to the function (required if there are no table arguments)
    :param schema: Schema within the SQL instance you want to access. If left blank we will default to the
        table.metadata.schema in the first Table passed to the function (required if there are no table arguments)
    :param kwargs: Any keyword arguments supported by the BaseOperator is supported (e.g ``queue``, ``owner``)
    :return: Transform functions return a ``Table`` object that can be passed to future tasks.
        This table will be either an auto-generated temporary table,
        or will overwrite a table given in the `output_table` parameter.
    """

    kwargs.update(
        {
            "op_kwargs": kwargs.get("op_kwargs", {}),
            "op_args": kwargs.get("op_args", {}),
        }
    )
    return NominalTransformOperator(
        conn_id=conn_id,
        parameters=parameters,
        handler=None,
        database=database,
        schema=schema,
        sql=file_path,
        python_callable=lambda: (file_path, parameters),
        **kwargs,
    ).output
